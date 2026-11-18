#!/usr/bin/env python3
"""UDP game server: player registration, state aggregation, snapshot broadcast.

Handles: sequence numbers, packet loss/duplicate/stale detection, idempotent
registration, player timeouts, and per-player latency / jitter / loss metrics.
"""
import argparse
import json
import random
import select
import signal
import socket
import time

from game_common import *


class Player:
    def __init__(self, pid, name, addr):
        self.pid, self.name, self.addr = pid, name, addr
        self.last_rx = time.monotonic()
        self.x, self.y = random.uniform(0, ARENA), random.uniform(0, ARENA)
        self.vx = self.vy = 0.0
        self.first_seq = self.last_seq = None
        self.rx = self.lost = self.dup = self.stale = 0
        self.jitter = Jitter()
        self.owd_up = []
        self.echo_seq = 0
        self.echo_ts = 0
        self.echo_rx_us = 0


class GameServer:
    def __init__(self, host, port, tick_hz, max_players, timeout, dscp, quiet):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((host, port))
        self.sock.setblocking(False)
        mark_dscp(self.sock, dscp)
        self.tick_hz, self.max_players, self.timeout, self.quiet = tick_hz, max_players, timeout, quiet
        self.players = {}          # pid -> Player
        self.by_addr = {}          # addr -> pid
        self.next_pid = 1
        self.snap_seq = 0
        self.tx_snaps = 0
        self.running = True
        self.log_events = []

    def log(self, msg):
        line = "[%.3f] %s" % (time.time(), msg)
        self.log_events.append(line)
        if not self.quiet:
            print(line, flush=True)

    def send(self, mtype, pid, seq, addr, payload=b"", flags=0):
        try:
            self.sock.sendto(pack(mtype, pid, seq, payload, flags), addr)
        except OSError:
            pass

    # ---------------------------------------------------------------- handlers
    def on_join(self, addr, payload):
        pid = self.by_addr.get(addr)
        if pid is None:                                   # new registration
            if len(self.players) >= self.max_players:
                self.send(T_JOIN_REJECT, 0, 0, addr, flags=REJ_FULL)
                self.log("JOIN rejected (server full) from %s:%d" % addr)
                return
            name = JOIN_PAYLOAD.unpack_from(payload)[0].rstrip(b"\0").decode(errors="replace") \
                if len(payload) >= JOIN_PAYLOAD.size else "player"
            pid = self.next_pid
            self.next_pid += 1
            self.players[pid] = Player(pid, name, addr)
            self.by_addr[addr] = pid
            self.log("JOIN  pid=%d name=%s addr=%s:%d (players=%d)" % (pid, name, addr[0], addr[1], len(self.players)))
        # duplicate JOINs (lost ACK) simply get the ACK again
        self.send(T_JOIN_ACK, pid, 0, addr, ACK_PAYLOAD.pack(self.tick_hz))

    def remove(self, pid, why):
        p = self.players.pop(pid, None)
        if p:
            self.by_addr.pop(p.addr, None)
            self.log("%s pid=%d name=%s" % (why, pid, p.name))

    def on_state(self, addr, pid, seq, ts, payload, rx_us):
        p = self.players.get(pid)
        if p is None or p.addr != addr:
            self.send(T_JOIN_REJECT, 0, 0, addr, flags=REJ_NOT_REGISTERED)   # tell client to re-register
            return
        p.last_rx = time.monotonic()
        if len(payload) < STATE_PAYLOAD.size:
            return
        if p.last_seq is None:
            p.first_seq, p.last_seq = seq, seq - 1
        if seq > p.last_seq:                               # new packet
            p.lost += seq - p.last_seq - 1                 # gap => presumed lost
            p.last_seq = seq
            p.rx += 1
            p.x, p.y, p.vx, p.vy = STATE_PAYLOAD.unpack_from(payload)
            p.echo_seq, p.echo_ts, p.echo_rx_us = seq, ts, rx_us
            p.jitter.update(ts, rx_us)
            p.owd_up.append(rx_us - ts)
        elif seq == p.last_seq:
            p.dup += 1                                     # duplicate
        else:
            p.stale += 1                                   # late / reordered: discard, state is old
            if p.lost > 0:
                p.lost -= 1                                # it was counted lost but did arrive

    def on_datagram(self, data, addr):
        rx_us = now_us()
        m = unpack(data)
        if not m:
            return
        t, f, pid, seq, ts, payload = m
        if t == T_JOIN:
            self.on_join(addr, payload)
        elif t == T_STATE:
            self.on_state(addr, pid, seq, ts, payload, rx_us)
        elif t == T_LEAVE:
            p = self.players.get(pid)
            if p and p.addr == addr:
                self.remove(pid, "LEAVE")
            self.send(T_LEAVE_ACK, pid, 0, addr)           # always ACK (idempotent)

    # --------------------------------------------------------------- broadcast
    def broadcast(self):
        if not self.players:
            return
        self.snap_seq += 1
        entries = b"".join(SNAP_ENTRY.pack(p.pid, p.x, p.y, p.vx, p.vy) for p in self.players.values())
        now = now_us()
        for p in list(self.players.values()):
            hold = max(0, now - p.echo_rx_us) if p.echo_rx_us else 0
            head = SNAP_HEAD.pack(p.echo_seq, p.echo_ts, min(hold, 0xFFFFFFFF), len(self.players))
            self.send(T_SNAPSHOT, p.pid, self.snap_seq, p.addr, head + entries)
            self.tx_snaps += 1

    def reap(self):
        now = time.monotonic()
        for pid in [pid for pid, p in self.players.items() if now - p.last_rx > self.timeout]:
            self.remove(pid, "TIMEOUT")

    # ------------------------------------------------------------------ report
    def report(self):
        out = {"snapshots_sent": self.tx_snaps, "players": {}, "events": self.log_events[-200:]}
        for p in self.players_seen.values():
            expected = (p.last_seq - p.first_seq + 1) if p.last_seq is not None else 0
            out["players"][str(p.pid)] = {
                "name": p.name, "state_rx": p.rx, "expected": expected, "lost": p.lost,
                "loss_pct": round(100.0 * p.lost / expected, 2) if expected else 0.0,
                "duplicates": p.dup, "stale_discarded": p.stale,
                "jitter_rfc3550_ms": round(p.jitter.rfc_ms, 3), "jitter_mean_ms": round(p.jitter.mean_ms, 3),
                "owd_up_ms": summarize(p.owd_up)}
        return out

    def run(self, duration, stats_interval):
        self.players_seen = {}
        signal.signal(signal.SIGINT, lambda *_: setattr(self, "running", False))
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "running", False))
        period = 1.0 / self.tick_hz
        start = time.monotonic()
        next_tick = start
        next_stats = start + stats_interval
        self.log("server up, tick=%dHz max_players=%d" % (self.tick_hz, self.max_players))
        while self.running and (duration <= 0 or time.monotonic() - start < duration):
            select.select([self.sock], [], [], max(0.0, min(next_tick - time.monotonic(), 0.05)))
            while True:
                try:
                    data, addr = self.sock.recvfrom(2048)
                except (BlockingIOError, ConnectionResetError):
                    break
                self.on_datagram(data, addr)
            self.players_seen.update(self.players)
            now = time.monotonic()
            if now >= next_tick:
                self.broadcast()
                self.reap()
                next_tick += period
                if now - next_tick > 5 * period:
                    next_tick = now + period
            if stats_interval > 0 and now >= next_stats:
                next_stats += stats_interval
                if not self.quiet:
                    print("[stats] players=%d snapshots_tx=%d" % (len(self.players), self.tx_snaps), flush=True)
        return self.report()


def main():
    ap = argparse.ArgumentParser(description="UDP multiplayer game server")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=GAME_PORT)
    ap.add_argument("--tick-hz", type=int, default=20)
    ap.add_argument("--max-players", type=int, default=16)
    ap.add_argument("--timeout", type=float, default=5.0, help="seconds of silence before a player is dropped")
    ap.add_argument("--dscp", type=int, default=DSCP_EF, help="DSCP marking for game packets (0 = off)")
    ap.add_argument("--duration", type=float, default=0, help="seconds to run (0 = until Ctrl-C)")
    ap.add_argument("--stats-interval", type=float, default=5.0)
    ap.add_argument("--out", help="write JSON report here on exit")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    srv = GameServer(a.host, a.port, a.tick_hz, a.max_players, a.timeout, a.dscp, a.quiet)
    rep = srv.run(a.duration, a.stats_interval)
    if a.out:
        with open(a.out, "w") as f:
            json.dump(rep, f, indent=2)
    if not a.quiet:
        print(json.dumps({k: v for k, v in rep.items() if k != "events"}, indent=2))


if __name__ == "__main__":
    main()
