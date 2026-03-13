# Omie Dashboard API

Backend FastAPI para dashboard executivo integrado à API do Omie.

## Rodar localmente

1. Crie e ative o ambiente virtual.
2. Instale dependências com `pip install -r requirements.txt`.
3. Copie `.env.example` para `.env` e preencha as variáveis.
4. Rode `uvicorn app.main:app --reload`.

## Deploy no Render

Use o `render.yaml` ou configure manualmente:
- Build command: `pip install -r requirements.txt`
- Start command: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`
