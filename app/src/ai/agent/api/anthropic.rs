/// Direct HTTP client for Anthropic's Messages API.
///
/// Bypasses Warp's servers entirely — traffic goes directly to api.anthropic.com.
/// Follows the same response-event format as the Ollama integration so the
/// rest of the agent pipeline requires no changes.
use std::sync::Arc;

use anyhow::anyhow;
use async_stream::stream;
use futures::StreamExt;
use prost_types::FieldMask;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use warp_multi_agent_api as api;

use crate::server::server_api::AIApiError;

use super::{AIAgentInput, Event, ResponseStream};

const ANTHROPIC_API_URL: &str = "https://api.anthropic.com/v1/messages";
const ANTHROPIC_VERSION_HEADER: &str = "2023-06-01";
const DEFAULT_MAX_TOKENS: u32 = 8192;

// --- Anthropic request/response types ---

#[derive(Serialize)]
struct MessagesRequest {
    model: String,
    max_tokens: u32,
    messages: Vec<ChatMessage>,
    stream: bool,
}

#[derive(Serialize, Clone)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct StreamEvent {
    #[serde(rename = "type")]
    event_type: String,
    delta: Option<ContentDelta>,
}

#[derive(Deserialize)]
struct ContentDelta {
    #[serde(rename = "type")]
    delta_type: String,
    text: Option<String>,
}

// --- Proto event builders (same helpers as ollama.rs) ---

fn response_event(r#type: api::response_event::Type) -> Event {
    Ok(api::ResponseEvent {
        r#type: Some(r#type),
    })
}

fn client_actions_event(actions: Vec<api::ClientAction>) -> Event {
    response_event(api::response_event::Type::ClientActions(
        api::response_event::ClientActions { actions },
    ))
}

fn client_action(action: api::client_action::Action) -> api::ClientAction {
    api::ClientAction {
        action: Some(action),
    }
}

fn agent_output_message(task_id: &str, message_id: &str, text: &str) -> api::Message {
    api::Message {
        id: message_id.to_string(),
        task_id: task_id.to_string(),
        message: Some(api::message::Message::AgentOutput(
            api::message::AgentOutput {
                text: text.to_string(),
            },
        )),
        ..Default::default()
    }
}

// --- Model ID normalization ---

/// Maps Warp's internal model aliases to real Anthropic model IDs.
fn normalize_model_id(model: &str) -> String {
    match model {
        "auto" | "claude-sonnet" | "" => "claude-sonnet-4-6".to_string(),
        "claude-opus" => "claude-opus-4-7".to_string(),
        "claude-haiku" => "claude-haiku-4-5-20251001".to_string(),
        other => other.to_string(),
    }
}

// --- Public API ---

/// Returns true when an Anthropic API key is available and the inputs contain a user query.
pub fn is_anthropic_request(inputs: &[AIAgentInput], anthropic_key: Option<&str>) -> bool {
    anthropic_key.is_some_and(|k| !k.is_empty())
        && inputs
            .iter()
            .any(|i| matches!(i, AIAgentInput::UserQuery { .. }))
}

/// Extracts chat messages from agent inputs for the Anthropic Messages API.
/// Preserves conversation history (user + assistant turns) when available.
fn extract_messages(inputs: &[AIAgentInput]) -> Vec<ChatMessage> {
    inputs
        .iter()
        .filter_map(|input| match input {
            AIAgentInput::UserQuery { query, .. } => Some(ChatMessage {
                role: "user".to_string(),
                content: query.clone(),
            }),
            _ => None,
        })
        .collect()
}

/// Streams a chat completion from Anthropic and emits `ResponseEvent`s matching
/// the format the Warp agent pipeline expects.
pub fn stream_response(
    api_key: String,
    model: String,
    inputs: Vec<AIAgentInput>,
) -> ResponseStream {
    let conversation_id = Uuid::new_v4().to_string();
    let task_id = Uuid::new_v4().to_string();
    let message_id = Uuid::new_v4().to_string();
    let model_id = normalize_model_id(&model);

    Box::pin(stream! {
        // 1. Signal stream start
        yield response_event(api::response_event::Type::Init(
            api::response_event::StreamInit {
                conversation_id,
                request_id: Uuid::new_v4().to_string(),
                run_id: String::new(),
            },
        ));

        // 2. Create task
        yield client_actions_event(vec![client_action(
            api::client_action::Action::CreateTask(api::client_action::CreateTask {
                task: Some(api::Task {
                    id: task_id.clone(),
                    description: "Anthropic response".to_string(),
                    ..Default::default()
                }),
            }),
        )]);

        // 3. Add initial empty assistant message
        yield client_actions_event(vec![client_action(
            api::client_action::Action::AddMessagesToTask(
                api::client_action::AddMessagesToTask {
                    task_id: task_id.clone(),
                    messages: vec![agent_output_message(&task_id, &message_id, "")],
                },
            ),
        )]);

        // 4. Call Anthropic Messages API
        let messages = extract_messages(&inputs);
        if messages.is_empty() {
            yield Err(Arc::new(AIApiError::Other(anyhow!(
                "No user message found for Anthropic"
            ))));
            return;
        }

        let body = MessagesRequest {
            model: model_id,
            max_tokens: DEFAULT_MAX_TOKENS,
            messages,
            stream: true,
        };

        let response = match reqwest::Client::new()
            .post(ANTHROPIC_API_URL)
            .header("x-api-key", &api_key)
            .header("anthropic-version", ANTHROPIC_VERSION_HEADER)
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                yield Err(Arc::new(AIApiError::Other(anyhow!(
                    "Anthropic connection failed: {e}"
                ))));
                return;
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            yield Err(Arc::new(AIApiError::Other(anyhow!(
                "Anthropic HTTP {status}: {text}"
            ))));
            return;
        }

        // 5. Stream SSE chunks → AppendToMessageContent events
        let mut byte_stream = response.bytes_stream();
        let mut buffer = String::new();

        while let Some(chunk) = byte_stream.next().await {
            let bytes = match chunk {
                Ok(b) => b,
                Err(e) => {
                    yield Err(Arc::new(AIApiError::Other(anyhow!(
                        "Anthropic stream error: {e}"
                    ))));
                    return;
                }
            };

            buffer.push_str(&String::from_utf8_lossy(&bytes));

            while let Some(pos) = buffer.find('\n') {
                let line = buffer[..pos].trim().to_string();
                buffer = buffer[pos + 1..].to_string();

                let Some(data) = line.strip_prefix("data: ") else {
                    continue;
                };

                let Ok(event) = serde_json::from_str::<StreamEvent>(data) else {
                    continue;
                };

                if event.event_type == "content_block_delta" {
                    if let Some(delta) = event.delta {
                        if delta.delta_type == "text_delta" {
                            if let Some(text) = delta.text {
                                if !text.is_empty() {
                                    yield client_actions_event(vec![client_action(
                                        api::client_action::Action::AppendToMessageContent(
                                            api::client_action::AppendToMessageContent {
                                                task_id: task_id.clone(),
                                                message: Some(agent_output_message(
                                                    &task_id,
                                                    &message_id,
                                                    &text,
                                                )),
                                                mask: Some(FieldMask {
                                                    paths: vec!["agent_output.text".to_string()],
                                                }),
                                            },
                                        ),
                                    )]);
                                }
                            }
                        }
                    }
                }

                if event.event_type == "message_stop" {
                    break;
                }
            }
        }

        // 6. Signal stream end
        yield response_event(api::response_event::Type::Finished(
            api::response_event::StreamFinished {
                reason: Some(api::response_event::stream_finished::Reason::Done(
                    api::response_event::stream_finished::Done {},
                )),
                ..Default::default()
            },
        ));
    })
}
