# Yahoo Finance MCP server - production image.
#
# Dependencies are installed from uv.lock while the image is BUILT. The
# previous image started with `uv run server.py`, which created a virtualenv
# and installed ~76 packages (dev tools included) every time the container
# started, so each cold start on Render took about two minutes and MCP
# clients timed out. Now a cold start only has to launch Python.
FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Runtime dependencies only (no dev tools), exactly as locked.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY server.py yahoo_data.py ./

ENV PATH="/app/.venv/bin:${PATH}" \
    MCP_TRANSPORT=http

# Fail the build if an import is broken; Render then keeps the running version.
RUN python -c "import curl_cffi, server, yahoo_data; print('import check ok')"

EXPOSE 10000
CMD ["python", "server.py"]
