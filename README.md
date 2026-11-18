Multiplayer Game Communication Network (UDP + SDN)

| Requirement | Where |
|---|---|
| UDP game-state updates | `game_client.py` sends STATE @20 Hz, `game_server.py` broadcasts SNAPSHOTs |
| Register multiple players | JOIN / JOIN_ACK / JOIN_REJECT (reliable, retry + back-off, max-players) |
| Sequence numbers | per-player STATE seq, global SNAPSHOT seq (both directions) |
| Handle packet loss | gap detection, stale/duplicate discard, dead-reckoning, rejoin, player timeout |
| Traffic prioritization | `game_qos_controller.py` (Ryu) -> OVS queue 1 for UDP:5000; clients mark DSCP EF |
| Measure latency | RTT (echo-based) and one-way delay, mean/p50/p95/p99 |
| Measure jitter | RFC 3550 jitter + mean delay variation |
| Test under congestion + loss | `traffic_gen.py` flood, `tc netem` loss; `run_mininet.py` scenarios, `test_local.py` |

## Quick test (no root, no Mininet)
    python3 test_local.py            # baseline vs congested-FIFO vs congested-priority + functional tests

## Full SDN run (Ubuntu + Mininet + Open vSwitch + Ryu)
    pip install ryu           # (or os-ken)
    ryu-manager game_qos_controller.py                       # terminal 1
    sudo python3 run_mininet.py --duration 20                # terminal 2 (all 9 scenarios)
    sudo python3 run_mininet.py --only 1,2,5,6 --loss 10     # pick scenarios / loss %

Scenarios: 1 baseline, 2 loss only, 3 congestion, 4 congestion+priority, 5 congestion+loss,
6 congestion+loss+priority, 7 congestion with auto mode, 8 light congestion, 9 heavy congestion.
Results: results/summary.json and results/summary.csv.
Mode file `/tmp/game_qos_mode` = `on` | `off` | `auto`. Controller events: `/tmp/game_qos_events.log`.

NOTE: run_mininet.py and game_qos_controller.py need Mininet/OVS/Ryu and were NOT run in the authoring environment.
One-way delay assumes hosts share a clock (true on one machine / Mininet); RTT and jitter do not.
