#!/usr/bin/env python3
"""Self-contained test of the whole game network on ONE machine (no Mininet / root needed).

A user-space network emulator sits between the clients and the server:

    client --> [relay] --> bottleneck Link (rate, delay, random loss, finite queue) --> server
    client <-- [relay] <-- bottleneck Link <------------------------------------------ server

The Link runs as a plain FIFO (no priority) or strict-priority (game class first), mimicking
what the SDN controller + OVS queues do in Mininet.  Phases: baseline, congested FIFO, congested priority.

Usage: python3 test_local.py [--duration 12] [--clients 4] [--loss 1]
"""
import argparse
import collections
import heapq
import json
import os
import random
import socket
import subprocess
import sys
import tempfile
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable


class Link:
    """One direction of a bottleneck link (queue limits counted in packets, like pfifo)."""

    def __init__(self, rate_bps, delay_s, loss, priority, q_noise=25, q_game=200, fifo_q=25):
        self.rate, self.delay, self.loss, self.priority = rate_bps, delay_s, loss, priority
        self.lim = {"game": q_game, "noise": q_noise}
        self.fifo_q = fifo_q
        self.q = {"game": collections.deque(), "noise": collections.deque(), "fifo": collections.deque()}
        self.qcount = collections.Counter()
        self.cv = threading.Condition()
        self.dcv = threading.Condition()
        self.heap, self.cnt, self.stop = [], 0, False
        self.dropped = collections.Counter()
        for fn in (self._tx, self._deliver):
            threading.Thread(target=fn, daemon=True).start()

    def submit(self, size, cls, deliver, data=None):
        if random.random() < self.loss:
            self.dropped[cls + "_random"] += 1
            return
        with self.cv:
            qn = cls if self.priority else "fifo"
            limit = self.lim[cls] if self.priority else self.fifo_q
            if self.qcount[qn] + 1 > limit:
                self.dropped[cls + "_queue"] += 1          # tail drop
                return
            self.q[qn].append((size, deliver, data))
            self.qcount[qn] += 1
            self.cv.notify()

    def _pick(self):
        for qn in (("game", "noise") if self.priority else ("fifo",)):
            if self.q[qn]:
                pkt = self.q[qn].popleft()
                self.qcount[qn] -= 1
                return pkt
        return None

    def _tx(self):
        free_at = time.monotonic()
        while not self.stop:
            with self.cv:
                pkt = self._pick()
                if pkt is None:
                    self.cv.wait(0.05)
                    continue
            start = max(time.monotonic(), free_at)
            free_at = start + pkt[0] * 8 / self.rate
            wait = free_at - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            with self.dcv:
                self.cnt += 1
                heapq.heappush(self.heap, (free_at + self.delay, self.cnt, pkt))
                self.dcv.notify()

    def _deliver(self):
        while not self.stop:
            with self.dcv:
                if not self.heap:
                    self.dcv.wait(0.05)
                    continue
                due = self.heap[0][0] - time.monotonic()
                if due > 0:
                    self.dcv.wait(min(due, 0.05))
                    continue
                _, _, (size, deliver, data) = heapq.heappop(self.heap)
            deliver(data)


class Emulator:
    def __init__(self, server_addr, n_clients, base_port, rate, delay, loss, priority, noise_bps):
        self.up = Link(rate, delay, loss, priority)
        self.down = Link(rate, delay, loss, priority)
        self.stop = False
        self.server_addr = server_addr
        self.socks = []
        for i in range(n_clients):
            self._relay(base_port + i)
        if noise_bps:
            for link in (self.up, self.down):
                threading.Thread(target=self._noise, args=(link, noise_bps), daemon=True).start()

    def _relay(self, port):
        lst = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        lst.bind(("127.0.0.1", port))
        lst.settimeout(0.2)
        upstream = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        upstream.settimeout(0.2)
        self.socks += [lst, upstream]
        caddr = {}

        def from_client():
            while not self.stop:
                try:
                    d, a = lst.recvfrom(2048)
                except (socket.timeout, OSError):
                    continue
                caddr["a"] = a
                self.up.submit(len(d) + 28, "game", lambda x: upstream.sendto(x, self.server_addr), d)

        def from_server():
            while not self.stop:
                try:
                    d, _ = upstream.recvfrom(2048)
                except (socket.timeout, OSError):
                    continue
                self.down.submit(len(d) + 28, "game", lambda x: lst.sendto(x, caddr["a"]) if "a" in caddr else None, d)

        for fn in (from_client, from_server):
            threading.Thread(target=fn, daemon=True).start()

    def _noise(self, link, bps, size=1200):
        pps = bps / 8 / size
        start, sent = time.monotonic(), 0
        while not self.stop:
            due = int((time.monotonic() - start) * pps) - sent
            for _ in range(min(due, 300)):
                link.submit(size, "noise", lambda _x: None)
                sent += 1
            time.sleep(0.001)

    def close(self):
        self.stop = True
        self.up.stop = self.down.stop = True
        time.sleep(0.3)
        for s in self.socks:
            s.close()


def run_phase(name, n, duration, priority, noise_mbps, loss, server_port, relay_base, tmp):
    srv_out = os.path.join(tmp, name + "_server.json")
    srv = subprocess.Popen([PY, os.path.join(HERE, "game_server.py"), "--host", "127.0.0.1", "--port", str(server_port),
                            "--duration", str(duration + 5), "--out", srv_out, "--quiet"])
    time.sleep(0.5)
    emu = Emulator(("127.0.0.1", server_port), n, relay_base, rate=1.5e6, delay=0.010, loss=loss,
                   priority=priority, noise_bps=noise_mbps * 1e6)
    procs, outs = [], []
    for i in range(n):
        out = os.path.join(tmp, "%s_c%d.json" % (name, i))
        outs.append(out)
        procs.append(subprocess.Popen([PY, os.path.join(HERE, "game_client.py"), "--server", "127.0.0.1:%d" % (relay_base + i),
                                       "--name", "p%d" % (i + 1), "--duration", str(duration), "--out", out, "--quiet"]))
    for p in procs:
        p.wait()
    srv.wait()
    emu.close()
    clients = [json.load(open(o)) for o in outs if os.path.exists(o)]
    return clients, json.load(open(srv_out))


def avg(xs):
    return sum(xs) / len(xs) if xs else 0.0


def summarize_phase(clients, server):
    return {
        "players": len({p["name"] for p in server["players"].values()}),
        "rtt_mean": avg([c["rtt_ms"]["mean"] for c in clients]),
        "rtt_p95": avg([c["rtt_ms"]["p95"] for c in clients]),
        "jitter": avg([c["jitter_rfc3550_ms"] for c in clients]),
        "loss_down": avg([c["loss_pct"] for c in clients]),
        "loss_up": avg([p["loss_pct"] for p in server["players"].values()]),
        "extrap": avg([c["extrapolated_ticks"] for c in clients]),
    }


def functional_tests(tmp):
    print("\n--- functional tests ---")
    ok = True
    port = 5301
    srv = subprocess.Popen([PY, os.path.join(HERE, "game_server.py"), "--host", "127.0.0.1", "--port", str(port),
                            "--max-players", "2", "--duration", "6", "--quiet", "--timeout", "1.5",
                            "--out", os.path.join(tmp, "full.json")])
    time.sleep(0.4)
    cl = [subprocess.Popen([PY, os.path.join(HERE, "game_client.py"), "--server", "127.0.0.1:%d" % port, "--name", "f%d" % i,
                            "--duration", "3", "--quiet"], stdout=subprocess.DEVNULL) for i in range(2)]
    time.sleep(1.0)
    third = subprocess.run([PY, os.path.join(HERE, "game_client.py"), "--server", "127.0.0.1:%d" % port, "--name", "late",
                            "--duration", "1", "--quiet"], capture_output=True, text=True)
    rejected = third.returncode != 0 and "registration failed" in third.stdout
    print("  [%s] 3rd player rejected when server is full (max 2)" % ("PASS" if rejected else "FAIL"))
    ok &= rejected
    for c in cl:
        c.wait()
    srv.wait()
    port = 5302
    srv = subprocess.Popen([PY, os.path.join(HERE, "game_server.py"), "--host", "127.0.0.1", "--port", str(port),
                            "--duration", "6", "--quiet", "--timeout", "1.5", "--out", os.path.join(tmp, "to.json")])
    time.sleep(0.4)
    c = subprocess.Popen([PY, os.path.join(HERE, "game_client.py"), "--server", "127.0.0.1:%d" % port, "--name", "crash",
                          "--duration", "30", "--quiet"])
    time.sleep(1.0)
    c.kill()
    srv.wait()
    ev = json.load(open(os.path.join(tmp, "to.json")))["events"]
    timed_out = any("TIMEOUT" in e for e in ev)
    print("  [%s] silent (crashed) player removed by server timeout" % ("PASS" if timed_out else "FAIL"))
    ok &= timed_out
    port = 5303
    srv = subprocess.Popen([PY, os.path.join(HERE, "game_server.py"), "--host", "127.0.0.1", "--port", str(port),
                            "--duration", "9", "--quiet", "--out", os.path.join(tmp, "rj1.json")])
    time.sleep(0.4)
    c = subprocess.Popen([PY, os.path.join(HERE, "game_client.py"), "--server", "127.0.0.1:%d" % port, "--name", "survivor",
                          "--duration", "8", "--out", os.path.join(tmp, "rj_c.json"), "--quiet"])
    time.sleep(1.5)
    srv.terminate()
    srv.wait()
    srv = subprocess.Popen([PY, os.path.join(HERE, "game_server.py"), "--host", "127.0.0.1", "--port", str(port),
                            "--duration", "5", "--quiet", "--out", os.path.join(tmp, "rj2.json")])
    c.wait()
    srv.wait()
    rep = json.load(open(os.path.join(tmp, "rj_c.json")))
    back = rep["rejoins"] >= 1 and len(json.load(open(os.path.join(tmp, "rj2.json")))["players"]) == 1
    print("  [%s] client re-registered after server restart (rejoins=%d)" % ("PASS" if back else "FAIL", rep["rejoins"]))
    ok &= back
    return ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=12)
    ap.add_argument("--clients", type=int, default=4)
    ap.add_argument("--loss", type=float, default=1.0, help="random loss %% on the emulated link (each direction)")
    ap.add_argument("--skip-functional", action="store_true")
    a = ap.parse_args()
    loss = a.loss / 100.0
    tmp = tempfile.mkdtemp(prefix="gamenet_")
    phases = [("baseline", False, 0.0), ("congested_FIFO", False, 2.2), ("congested_PRIORITY", True, 2.2)]
    res = {}
    for i, (name, prio, noise) in enumerate(phases):
        print("running phase '%s' (noise=%.1f Mbps, priority=%s, %ds)..." % (name, noise, prio, a.duration), flush=True)
        cl, sv = run_phase(name, a.clients, a.duration, prio, noise, loss, 5100 + i, 7100 + i * 20, tmp)
        res[name] = summarize_phase(cl, sv)
    print("\nBottleneck: 1.5 Mbps each way, 10 ms one-way delay, %.1f%% random loss; %d players @ 20 Hz" % (a.loss, a.clients))
    print("%-20s %7s %9s %9s %10s %9s %9s %8s" % ("phase", "players", "RTT(ms)", "RTTp95", "jitter(ms)", "loss_dn%", "loss_up%", "extrap"))
    for n, r in res.items():
        print("%-20s %7d %9.1f %9.1f %10.2f %9.2f %9.2f %8.0f" % (n, r["players"], r["rtt_mean"], r["rtt_p95"], r["jitter"],
                                                                  r["loss_down"], r["loss_up"], r["extrap"]))
    b, f, p = res["baseline"], res["congested_FIFO"], res["congested_PRIORITY"]
    checks = [
        ("all players registered in every phase", all(r["players"] == a.clients for r in res.values())),
        ("baseline has measurable RTT/jitter", b["rtt_mean"] > 15 and b["jitter"] >= 0),
        ("congestion w/o priority degrades RTT (>2x baseline)", f["rtt_mean"] > 2 * b["rtt_mean"]),
        ("congestion w/o priority increases loss", f["loss_down"] + f["loss_up"] > b["loss_down"] + b["loss_up"] + 3),
        ("priority keeps RTT close to baseline (<1.5x)", p["rtt_mean"] < 1.5 * b["rtt_mean"] + 5),
        ("priority reduces RTT vs FIFO (>=3x better)", p["rtt_mean"] * 3 <= f["rtt_mean"]),
        ("priority keeps jitter below FIFO jitter", p["jitter"] < f["jitter"]),
    ]
    print()
    allok = True
    for n, ok in checks:
        print("  [%s] %s" % ("PASS" if ok else "FAIL", n))
        allok &= ok
    if not a.skip_functional:
        allok &= functional_tests(tmp)
    print("\nOVERALL:", "ALL TESTS PASSED" if allok else "SOME TESTS FAILED")
    sys.exit(0 if allok else 1)


if __name__ == "__main__":
    main()
