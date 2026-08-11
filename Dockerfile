FROM python:3.13-slim

WORKDIR /app

# The engine, both HTTP surfaces, the Redis store and the S3 client are standard
# library only. This image installs two extras because compose runs the FastAPI
# service layer against MySQL: `api` (fastapi + uvicorn) and `mysql` (pymysql).
# Without them the same image still works — `serve` falls back to the built-in
# asyncio server and paused runs go to Redis.
COPY pyproject.toml README.md ./
COPY flowforge ./flowforge
COPY examples ./examples

RUN pip install --no-cache-dir ".[api,mysql]"

RUN useradd --create-home --uid 10001 flowforge && chown -R flowforge /app
USER flowforge

EXPOSE 8000
CMD ["python", "-m", "flowforge", "serve", "examples", "--host", "0.0.0.0", "--port", "8000"]
