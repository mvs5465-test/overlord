FROM python:3.11-slim

WORKDIR /app

COPY pyproject.toml README.md app.py ./
COPY overlord ./overlord

RUN pip install --no-cache-dir .

ENV HOST=0.0.0.0
ENV PORT=8080

ENTRYPOINT ["python", "app.py"]

