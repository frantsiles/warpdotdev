/// Direct HTTP client for Ollama's OpenAI-compatible API.
///
/// Bypasses Warp's servers entirely — all traffic stays on localhost.
/// Ollama must be running at the configured base URL (default: http://localhost:11434).
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

// --- OpenAI-compatible request/response types ---

#[derive(Serialize)]
struct ChatRequest {
    model: String,
    messages: Vec<ChatMessage>,
    stream: bool,
}

#[derive(Serialize, Clone)]
struct ChatMessage {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct ChatCompletionChunk {
    choices: Vec<ChunkChoice>,
}

#[derive(Deserialize)]
struct ChunkChoice {
    delta: Delta,
}

#[derive(Deserialize, Default)]
struct Delta {
    content: Option<String>,
}

// --- Proto event builders ---

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

// --- Public API ---

/// Returns true if Ollama is configured and the request should be routed locally.
pub fn is_ollama_request(inputs: &[AIAgentInput], ollama_base_url: Option<&str>) -> bool {
    ollama_base_url.is_some()
        && inputs
            .iter()
            .any(|i| matches!(i, AIAgentInput::UserQuery { .. }))
}

/// Extracts (role, content) message pairs from agent inputs for the Ollama API.
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

/// Streams a chat completion from Ollama and emits `ResponseEvent`s matching
/// the format the Warp agent pipeline expects.
pub fn stream_response(
    base_url: String,
    model: String,
    inputs: Vec<AIAgentInput>,
) -> ResponseStream {
    let conversation_id = Uuid::new_v4().to_string();
    let task_id = Uuid::new_v4().to_string();
    let message_id = Uuid::new_v4().to_string();

    Box::pin(stream! {
        // 1. Signal stream start
        yield response_event(api::response_event::Type::Init(
            api::response_event::StreamInit {
                conversation_id,
                request_id: Uuid::new_v4().to_string(),
                run_id: String::new(),
            },
        ));

        // 2. Create a task to hold the response
        yield client_actions_event(vec![client_action(
            api::client_action::Action::CreateTask(api::client_action::CreateTask {
                task: Some(api::Task {
                    id: task_id.clone(),
                    description: "Ollama response".to_string(),
                    ..Default::default()
                }),
            }),
        )]);

        // 3. Add the initial (empty) assistant message
        yield client_actions_event(vec![client_action(
            api::client_action::Action::AddMessagesToTask(
                api::client_action::AddMessagesToTask {
                    task_id: task_id.clone(),
                    messages: vec![agent_output_message(&task_id, &message_id, "")],
                },
            ),
        )]);

        // 4. Call Ollama
        let messages = extract_messages(&inputs);
        if messages.is_empty() {
            yield Err(Arc::new(AIApiError::Other(anyhow!("No user message found for Ollama"))));
            return;
        }

        let url = format!("{}/v1/chat/completions", base_url.trim_end_matches('/'));
        let body = ChatRequest { model, messages, stream: true };

        let response = match reqwest::Client::new()
            .post(&url)
            .json(&body)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                yield Err(Arc::new(AIApiError::Other(anyhow!("Ollama connection failed: {e}"))));
                return;
            }
        };

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            yield Err(Arc::new(AIApiError::Other(
                anyhow!("Ollama HTTP {status}: {text}"),
            )));
            return;
        }

        // 5. Stream SSE chunks → AppendToMessageContent events
        let mut byte_stream = response.bytes_stream();
        let mut buffer = String::new();

        while let Some(chunk) = byte_stream.next().await {
            let bytes = match chunk {
                Ok(b) => b,
                Err(e) => {
                    yield Err(Arc::new(AIApiError::Other(anyhow!("Ollama stream error: {e}"))));
                    return;
                }
            };

            buffer.push_str(&String::from_utf8_lossy(&bytes));

            // Drain complete lines from the buffer
            while let Some(pos) = buffer.find('\n') {
                let line = buffer[..pos].trim().to_string();
                buffer = buffer[pos + 1..].to_string();

                let Some(data) = line.strip_prefix("data: ") else { continue };
                if data == "[DONE]" { break; }

                let Ok(chunk) = serde_json::from_str::<ChatCompletionChunk>(data) else { continue };

                for choice in chunk.choices {
                    if let Some(text) = choice.delta.content {
                        if text.is_empty() { continue; }
                        yield client_actions_event(vec![client_action(
                            api::client_action::Action::AppendToMessageContent(
                                api::client_action::AppendToMessageContent {
                                    task_id: task_id.clone(),
                                    message: Some(agent_output_message(
                                        &task_id, &message_id, &text,
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
