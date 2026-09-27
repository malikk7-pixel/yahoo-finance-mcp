# Lessons

- After merging packaging metadata into a uv project, run `uv lock` and then `uv lock --check` before committing.
- A local `.venv` can make source-distribution builds fail on absolute interpreter symlinks; verify the wheel build separately and test sdist creation from a clean source tree.
- Never start the container with `uv run`: it builds a virtualenv and installs every locked package (dev tools included) at each start. Install with `uv sync --frozen --no-dev` in the Dockerfile and run `python server.py`.
- Never call yfinance synchronously inside an async MCP tool: one slow Yahoo request blocks every other request. Run it in the worker pool (`yahoo_data.cached_call`).
- Register `Future.add_done_callback` outside any lock the callback takes: an already-finished future runs the callback immediately, in the calling thread.
