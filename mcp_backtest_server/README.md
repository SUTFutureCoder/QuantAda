# QuantAda Backtest MCP Server (Minimal)

This package provides a backtest-only MCP server over stdio.
It does not import or link against the QuantAda framework code. It only executes a configured CLI runner.

## Run

```bash
python -m mcp_backtest_server
```

## Example Client (stdio)

```bash
set MCP_BACKTEST_SERVER_CMD=python -m mcp_backtest_server
python mcp_backtest_server/example_client.py
```

Minimal MCP config example: see `mcp_config.example.json`.

## Configuration

Environment variables:

- `MCP_BACKTEST_CMD_JSON` (preferred): JSON array command, e.g. `["python","E:\\Lin\\Github\\QuantAda\\run.py"]`
- `MCP_BACKTEST_CMD`: command string (split by shell rules), e.g. `python E:\\Lin\\Github\\QuantAda\\run.py`
- `MCP_BACKTEST_WORKDIR`: working directory for the runner
- `MCP_BACKTEST_DATA_DIR`: job storage directory
- `MCP_BACKTEST_ALLOWED_FLAGS`: optional allowlist for CLI flags (comma-separated). If set, any flag not in the list is rejected.
- `MCP_BACKTEST_DEDUPE`: if set to `1`, auto-dedupes requests by hashing runner + args + env overrides.
- `MCP_BACKTEST_MAX_CONCURRENT`: if > 0, limits concurrent runs and queues excess jobs.
- `MCP_BACKTEST_SCHED_INTERVAL_SEC`: scheduler interval when concurrency limit is enabled (default 1s).

If no command is provided, the default is:

```
<sys.executable> <repo_root>/run.py
```

## Tools

- `backtest_run`
  - Starts a backtest/optimizer run via the configured CLI runner
  - Blocks `--connect` (live trading)
  - Adds `--no_plot` unless `allow_plot=true`
- `backtest_status`
- `backtest_result`
- `backtest_cancel`
- `backtest_list`

## Notes

- Logs are written per job: `stdout.log` and `stderr.log`
- Job metadata is written to `result.json` in the job folder
- This server is backtest only. It does not expose live trading.
