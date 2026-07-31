FROM python:3.13-slim
ARG GIT_COMMIT=unknown-container-build
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY pyproject.toml README.md /app/
COPY src /app/src
RUN python -m pip install --no-cache-dir .
COPY configs /app/configs
RUN printf '%s\n' "$GIT_COMMIT" > /app/.release-commit
RUN mkdir -p /app/data && chown -R 65532:65532 /app/data
USER 65532:65532
ENTRYPOINT ["polymarket-bt"]
CMD ["collect", "--config", "configs/collector.yaml"]
