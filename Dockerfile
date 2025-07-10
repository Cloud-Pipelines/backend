# Build pipeline-studio-app from GitHub
FROM node:22 as builder
WORKDIR /app

RUN git clone https://github.com/Cloud-Pipelines/pipeline-studio-app.git .

RUN npm install
RUN echo VITE_GIT_COMMIT=\"$(git rev-parse --short HEAD | tr -d "\n")\" >.env
RUN npm run build


FROM python:3.12-slim-bullseye


# Install uv.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy the application into the container.
COPY . /app

# Install the application dependencies.
WORKDIR /app
RUN uv sync --frozen --no-cache

RUN apt-get -y update; apt-get -y install curl unzip
RUN curl -L https://github.com/Cloud-Pipelines/pipeline-editor/archive/refs/heads/gh-pages.zip -o pipeline-editor-gh-pages.zip && unzip pipeline-editor-gh-pages.zip

# RUN curl -L https://github.com/Cloud-Pipelines/pipeline-studio-app/archive/refs/heads/gh-pages.zip -o pipeline-studio-app-gh-pages.zip && unzip pipeline-studio-app-gh-pages.zip
COPY --from=builder /app/dist pipeline-studio-app-build

# Installing gke-gcloud-auth-plugin
## curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
## echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
#RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && apt-get update -y && apt-get install google-cloud-cli -y
# apt-get update && apt-get install -y google-cloud-cli-gke-gcloud-auth-plugin

# Installing gke-gcloud-auth-plugin replacement (The original gke-gcloud-auth-plugin installation uses almost a gigabyte).
RUN pip install google-auth requests
COPY ./gke-gcloud-auth-plugin.py /usr/local/bin/gke-gcloud-auth-plugin

CMD ["/app/.venv/bin/fastapi", "run", "api_server_main.py", "--port", "8000"]
