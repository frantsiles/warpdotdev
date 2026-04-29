# CLAUDE.md — Notas de trabajo para este fork

Este repositorio es un fork de [warpdotdev/warp](https://github.com/warpdotdev/warp.git).

## Remotes configurados

| Remote | URL |
|--------|-----|
| `origin` | https://github.com/frantsiles/warpdotdev |
| `upstream` | https://github.com/warpdotdev/warp.git |

## Sincronizar fork con el repo original (rebase)

Cuando el usuario diga **"haz un rebase con el repo original"** o similar, ejecutar estos comandos en orden:

```bash
git fetch upstream
git checkout master
git rebase upstream/master
git push origin master --force-with-lease
```

### Qué hace cada comando

| Comando | Descripción |
|---------|-------------|
| `git fetch upstream` | Descarga los cambios del repo original sin tocar el código local |
| `git checkout master` | Cambia a la rama principal del fork |
| `git rebase upstream/master` | Aplica los commits del upstream sobre master, historial lineal |
| `git push origin master --force-with-lease` | Sube los cambios al fork en GitHub (requiere force por el rebase) |

> **Nota:** `--force-with-lease` es más seguro que `--force` porque falla si alguien más subió cambios mientras tanto.

## Crear una rama nueva para trabajo propio

Siempre crear ramas desde `master` ya sincronizado:

```bash
git checkout master
git checkout -b nombre-de-la-rama
```

## Flujo completo recomendado antes de empezar a trabajar

```bash
git fetch upstream
git checkout master
git rebase upstream/master
git push origin master --force-with-lease
git checkout -b mi-nueva-rama
```
