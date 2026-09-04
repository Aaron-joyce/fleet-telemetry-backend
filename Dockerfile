FROM python:3.13-slim

# Copy uv binary from official Astral image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Environment configuration
ENV PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1

# Copy project dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies without dev packages
RUN uv sync --frozen --no-dev

# Copy source code and data files
COPY app/ ./app/
COPY robot/ ./robot/
COPY data/ ./data/

# Expose default HTTP port
EXPOSE 8000

# Default command for the backend service
CMD ["uv", "run", "python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
