# ABC Kill Scenario Testing

This package adds 3-node cluster helpers for the NCloud tablet replication path.

## What is included
- `start_abc_cluster.sh`
- `stop_abc_cluster.sh`
- `abc_kill_scenario_test.py`
- `coordinator/coordinator_abc.conf` is generated automatically by the start script

## Build first
```bash
make -C kvstore clean && make -C kvstore
make -C coordinator clean && make -C coordinator
```

## Start the 3-node cluster
```bash
./stop_abc_cluster.sh
rm -rf /tmp/pc_abc_cluster
./start_abc_cluster.sh
```

## Run the scenario test
```bash
python3 abc_kill_scenario_test.py
```

Expected ending:
```text
=== ABC KILL SCENARIO TEST PASSED ===
```

## What the test does
1. Writes on node1 and verifies replication to node2 and node3.
2. Kills node1 and waits for the coordinator to promote node2 or node3.
3. Verifies reads/writes still work on the promoted primary.
4. Kills the remaining secondary and verifies the last live replica still serves requests.
5. Restarts node1, waits for rejoin as secondary, writes again, and verifies the restarted node sees the new value.

## If it fails
Send these logs:
- `node1.log`
- `node2.log`
- `node3.log`
- `coordinator.log`
