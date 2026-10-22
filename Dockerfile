FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

RUN apt-get install -y nodejs npm

ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh

ENV PATH="/root/.local/bin/:$PATH"

# Copy dependency files first to leverage Docker cache
COPY backend/pyproject.toml backend/uv.lock* ./
RUN uv sync --frozen

# Copy the rest of the code
COPY . . 

EXPOSE 8000

WORKDIR /app/frontend
RUN npm install 
RUN npm run build
WORKDIR /app/backend
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]