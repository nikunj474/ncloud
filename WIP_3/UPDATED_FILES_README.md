# Updated files bundle

This zip contains the updated project files from the recent fix pass.

## Included updates

### Frontend
- Admin console tablet placement
- SSE/chunked transfer fix
- Non-blocking external SMTP send path
- Outbound SMTP helper
- macOS resolver link flag in Makefile

### Coordinator
- TABLETS endpoint
- recovery-path fix for recovered-primary promotion

### KV server
- Multi-tablet-per-server support
- request routing by row range
- repeatable `--tablet-spec name:start:end` CLI
- aggregate checkpoint/stats across hosted tablets
- backward-compatible single-tablet behavior

## Important note about replication + multi-tablet
The KV refactor is backward-compatible for the current replicated single-tablet deployment.

For safety, this implementation allows:
- replicated mode with exactly one hosted tablet per server process
- multi-tablet hosting in non-replicated mode

Fully replicated multi-tablet-per-process support would require a larger replication-protocol refactor so recovery/sync commands can identify tablets explicitly.

## Quick local proof script
Use:
- `tests/multi_tablet_local_test.py`

to demonstrate one KV process hosting two tablets and serving both ranges correctly.
