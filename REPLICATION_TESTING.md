# Replication Test Startup

The replication surface tests (`repl_write_surface_test.py` and
`repl_secondary_failure_test.py`) assume that you already have a **replicated**
2-node KV cluster running on these **client** ports:

- primary client: `5500`
- secondary client: `5501`

The actual replication traffic uses separate **replication** ports:

- primary repl: `5600`
- secondary repl: `5601`

## Correct startup

```bash
./start_replication_test_cluster.sh
```

This launches:

- secondary: `./kvstore/kvserver --port 5501 ... --node-id node2 --repl-port 5601`
- primary: `./kvstore/kvserver --port 5500 ... --node-id node1 --repl-port 5600 --replica node2@127.0.0.1:5601`

Then run:

```bash
python3 repl_write_surface_test.py
python3 repl_secondary_failure_test.py
```

## Why the earlier test failed

Starting two KV servers like this:

```bash
./kvstore/kvserver --port 5500 ...
./kvstore/kvserver --port 5501 ...
```

puts both nodes in **standalone mode**. No peer list is configured, so the
primary has nowhere to forward writes. The secondary's client port (`5501`) is
not the replication port. Replication must target the peer's `--repl-port`.

## Stop cluster

```bash
./stop_replication_test_cluster.sh
```
