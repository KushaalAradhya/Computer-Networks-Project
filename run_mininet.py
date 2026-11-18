#!/usr/bin/env python3
"""Mininet experiment: 4 players + background-traffic host on s1, game server + sink on s2,
10 Mbps bottleneck between the switches with OVS queues (queue 1 = game).

  Terminal 1:  ryu-manager game_qos_controller.py
  Terminal 2:  sudo python3 run_mininet.py [--duration 20] [--noise-mbps 14] [--loss 5] [--cli]

Scenarios (congestion x random packet loss x priority) are listed in scenarios() below.
  --loss N        random loss % injected on player uplinks and server downlink (default 5)
  --noise-mbps N  background flood rate (default 14 on a 10 Mbps bottleneck)
  --only a,b      run only the named scenarios (e.g. --only 1,2,5,6)
"""
import argparse
import json
import os
import time

from mininet.link import Link
from mininet.log import setLogLevel
from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI

HERE = os.path.dirname(os.path.abspath(__file__))
MODE_FILE = os.environ.get("GAME_QOS_MODE_FILE", "/tmp/game_qos_mode")
OUT = os.path.join(HERE, "results")
CAP = 10_000_000


def set_qos(sw, intf):
    sw.cmd("ovs-vsctl -- set port %s qos=@q -- --id=@q create qos type=linux-htb other-config:max-rate=%d "
           "queues:0=@q0 queues:1=@q1 -- --id=@q0 create queue other-config:min-rate=1000000 other-config:max-rate=%d "
           "other-config:priority=1 -- --id=@q1 create queue other-config:min-rate=3000000 other-config:max-rate=%d "
           "other-config:priority=0" % (intf, CAP, CAP, CAP))


def build():
    net = Mininet(controller=None, switch=lambda n, **k: OVSSwitch(n, protocols="OpenFlow13", **k), link=Link)
    net.addController("c0", controller=RemoteController, ip="127.0.0.1", port=6653)
    s1, s2 = net.addSwitch("s1"), net.addSwitch("s2")
    players = [net.addHost("p%d" % i) for i in range(1, 5)]
    noise = net.addHost("noise")
    srv, sink = net.addHost("srv"), net.addHost("sink")
    for h in players + [noise]:
        net.addLink(h, s1)
    for h in (srv, sink):
        net.addLink(h, s2)
    net.addLink(s1, s2)
    net.start()
    for a, b in ((s1, s2), (s2, s1)):
        set_qos(a, a.connectionsTo(b)[0][0].name)
    return net, players, noise, srv, sink


def set_loss(hosts, loss_pct):
    """Inject random packet loss with tc netem on each host's egress interface.
    Players' egress = uplink loss, server egress = downlink (snapshot) loss."""
    for h in hosts:
        intf = h.defaultIntf().name
        if loss_pct > 0:
            h.cmd("tc qdisc replace dev %s root netem loss %s%%" % (intf, loss_pct))
        else:
            h.cmd("tc qdisc del dev %s root 2>/dev/null" % intf)


def phase(net, players, noise, srv, sink, name, mode, noise_mbps, loss_pct, duration):
    open(MODE_FILE, "w").write(mode)
    set_loss(players + [srv], loss_pct)
    time.sleep(3)                                       # let controller poll + flush flows
    print("== %s (priority=%s, noise=%s Mbps, loss=%s%%)" % (name, mode, noise_mbps, loss_pct), flush=True)
    os.makedirs(OUT, exist_ok=True)
    tag = os.path.join(OUT, name)
    sp = srv.popen("python3 %s/game_server.py --duration %d --out %s_server.json --quiet" % (HERE, duration + 6, tag))
    time.sleep(1)
    procs = []
    if noise_mbps:
        procs.append(sink.popen("python3 %s/traffic_gen.py sink --duration %d" % (HERE, duration + 2)))
        time.sleep(0.5)
        procs.append(noise.popen("python3 %s/traffic_gen.py send --dst %s --mbps %s --duration %d" % (HERE, sink.IP(), noise_mbps, duration)))
    cl = [h.popen("python3 %s/game_client.py --server %s:5000 --name %s --duration %d --out %s_%s.json --quiet" % (
        HERE, srv.IP(), h.name, duration, tag, h.name)) for h in players]
    for p in cl + procs:
        p.wait()
    sp.wait()
    reps = [json.load(open("%s_%s.json" % (tag, h.name))) for h in players if os.path.exists("%s_%s.json" % (tag, h.name))]
    srep = json.load(open(tag + "_server.json")) if os.path.exists(tag + "_server.json") else {"players": {}}
    n = max(1, len(reps))
    ups = [p["loss_pct"] for p in srep["players"].values()]
    return {"phase": name, "priority": mode, "noise_mbps": noise_mbps, "loss_set_pct": loss_pct, "players": len(reps),
            "rtt_ms": sum(r["rtt_ms"]["mean"] for r in reps) / n, "rtt_p95_ms": sum(r["rtt_ms"]["p95"] for r in reps) / n,
            "jitter_ms": sum(r["jitter_rfc3550_ms"] for r in reps) / n,
            "loss_down_pct": sum(r["loss_pct"] for r in reps) / n,
            "loss_up_pct": (sum(ups) / len(ups)) if ups else 0.0,
            "extrapolated_ticks": sum(r["extrapolated_ticks"] for r in reps) / n,
            "rejoins": sum(r["rejoins"] for r in reps)}


def scenarios(noise, loss):
    """(name, priority mode, noise Mbps, random loss %)"""
    return [
        ("1_baseline",              "off",  0,     0),     # no congestion, no loss
        ("2_loss_only",             "off",  0,     loss),  # packet loss, no congestion
        ("3_congestion",            "off",  noise, 0),     # congestion, game traffic NOT prioritized
        ("4_congestion_priority",   "on",   noise, 0),     # same congestion, SDN prioritization ON
        ("5_congestion_loss",       "off",  noise, loss),  # worst case, no priority
        ("6_congestion_loss_prio",  "on",   noise, loss),  # worst case, with SDN priority
        ("7_congestion_auto",       "auto", noise, 0),     # controller turns priority on by itself
        ("8_light_congestion",      "off",  7,     0),     # below capacity: little effect expected
        ("9_heavy_congestion",      "off",  25,    0),     # 2.5x capacity
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=int, default=20)
    ap.add_argument("--noise-mbps", type=float, default=14)
    ap.add_argument("--loss", type=float, default=5, help="random packet loss %% for the loss scenarios")
    ap.add_argument("--only", default="", help="comma-separated scenario names (or numbers) to run")
    ap.add_argument("--cli", action="store_true")
    a = ap.parse_args()
    setLogLevel("info")
    net, players, noise, srv, sink = build()
    try:
        time.sleep(2)
        net.ping([players[0], srv])                     # warm up ARP / learning
        if a.cli:
            CLI(net)
            return
        chosen = scenarios(a.noise_mbps, a.loss)
        if a.only:
            want = [w.strip() for w in a.only.split(",")]
            chosen = [c for c in chosen if c[0] in want or c[0].split("_")[0] in want]
        rows = [phase(net, players, noise, srv, sink, n, m, nm, ls, a.duration) for n, m, nm, ls in chosen]
        set_loss(players + [srv], 0)
        print("\n%-24s %5s %6s %5s | %8s %8s %10s %8s %8s %7s" % (
            "scenario", "prio", "noise", "loss", "RTT(ms)", "p95(ms)", "jitter(ms)", "loss_dn%", "loss_up%", "extrap"))
        for r in rows:
            print("%-24s %5s %6.0f %5.1f | %8.1f %8.1f %10.2f %8.2f %8.2f %7.0f" % (
                r["phase"], r["priority"], r["noise_mbps"], r["loss_set_pct"], r["rtt_ms"], r["rtt_p95_ms"],
                r["jitter_ms"], r["loss_down_pct"], r["loss_up_pct"], r["extrapolated_ticks"]))
        json.dump(rows, open(os.path.join(OUT, "summary.json"), "w"), indent=2)
        with open(os.path.join(OUT, "summary.csv"), "w") as f:
            keys = list(rows[0].keys())
            f.write(",".join(keys) + "\n")
            for r in rows:
                f.write(",".join(str(round(r[k], 3)) if isinstance(r[k], float) else str(r[k]) for k in keys) + "\n")
        print("\nSaved results/summary.json and results/summary.csv")
    finally:
        set_loss(players + [srv], 0)
        net.stop()


if __name__ == "__main__":
    main()
