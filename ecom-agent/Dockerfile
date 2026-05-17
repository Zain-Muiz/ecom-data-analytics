FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=8080
ENV GOOGLE_GENAI_USE_VERTEXAI=TRUE

# ADK ships its own FastAPI server with a chat UI at /
CMD ["sh", "-c", "adk web --host 0.0.0.0 --port ${PORT} ."]
