#!/usr/bin/env python3
"""UDP game client (bot player).

Sends STATE updates at a fixed rate, receives SNAPSHOTs, and measures
RTT, one-way delay, jitter (RFC 3550) and packet loss.  Loss handling:
  * reliable registration/leave (retry with exponential back-off)
  * sequence numbers: duplicates and stale/out-of-order snapshots discarded
  * dead-reckoning (extrapolation) of remote players when snapshots are missing
  * automatic re-registration if the server goes silent / forgets us
"""
import argparse
import json
import random
import select
import signal
import socket
import time

from game_common import *


class GameClient:
    def __init__(self, server, name, hz, dscp, quiet):
        host, port = server.rsplit(":", 1)
        self.server = (host, int(port))
        self.name, self.hz, self.quiet = name, hz, quiet
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setblocking(False)
        mark_dscp(self.sock, dscp)
        self.pid = 0
        self.seq = 0
        self.running = True
        self.need_rejoin = False
        # bot state
        self.x, self.y = random.uniform(0, ARENA), random.uniform(0, ARENA)
        self.vx, self.vy = random.uniform(-60, 60), random.uniform(-60, 60)
        # remote world view
        self.remote = {}                     # pid -> [x, y, vx, vy, last_update_monotonic]
        self.view = {}                       # extrapolated positions
        # metrics
        self.sent = 0
        self.snap_rx = self.snap_lost = self.snap_dup = self.snap_stale = 0
        self.snap_last = None
        self.jitter = Jitter()
        self.rtt, self.owd_down = [], []
        self.last_echo = 0
        self.extrap_ticks = 0
        self.join_retries = 0
        self.rejoins = 0
        self.last_snap_rx = time.monotonic()
        self.snap_period = 1.0 / 20

    def send(self, mtype, seq=0, payload=b"", flags=0):
        try:
            self.sock.sendto(pack(mtype, self.pid, seq, payload, flags), self.server)
        except OSError:
            pass

    def _wait_for(self, want_types, timeout):
        deadline = time.monotonic() + timeout
        while True:
            rem = deadline - time.monotonic()
            if rem <= 0:
                return None
            if not select.select([self.sock], [], [], rem)[0]:
                return None
            try:
                data, _ = self.sock.recvfrom(2048)
            except (BlockingIOError, ConnectionResetError):
                continue
            m = unpack(data)
            if m and m[0] in want_types:
                return m

    def register(self):
        delay = 0.3
        for _ in range(30):
            self.send(T_JOIN, 0, JOIN_PAYLOAD.pack(self.name.encode()[:16]))
            m = self._wait_for((T_JOIN_ACK, T_JOIN_REJECT), delay)
            if m:
                if m[0] == T_JOIN_ACK:
                    self.pid = m[2]
                    self.snap_period = 1.0 / max(1, ACK_PAYLOAD.unpack_from(m[5])[0])
                    self.last_snap_rx = time.monotonic()
                    return True
                if m[1] == REJ_FULL:
                    return False
            self.join_retries += 1
            delay = min(delay * 2, 1.0)
        return False

    def leave(self):
        delay = 0.2
        for _ in range(4):
            self.send(T_LEAVE)
            if self._wait_for((T_LEAVE_ACK,), delay):
                return True
            delay *= 2
        return False

    # --------------------------------------------------------------- receiving
    def on_snapshot(self, seq, ts, payload):
        rx_us = now_us()
        if self.snap_last is None or seq < self.snap_last - 1000:    # first packet / server restarted
            self.snap_last = seq - 1
        if seq > self.snap_last:
            self.snap_lost += seq - self.snap_last - 1
            self.snap_last = seq
            self.snap_rx += 1
            self.last_snap_rx = time.monotonic()
            self.owd_down.append(rx_us - ts)
            self.jitter.update(ts, rx_us)
            echo_seq, echo_ts, hold_us, count = SNAP_HEAD.unpack_from(payload)
            if echo_seq > self.last_echo:                            # RTT = now - our send time - server hold
                self.rtt.append(max(0, rx_us - echo_ts - hold_us))
                self.last_echo = echo_seq
            off = SNAP_HEAD.size
            for _ in range(count):
                pid, x, y, vx, vy = SNAP_ENTRY.unpack_from(payload, off)
                off += SNAP_ENTRY.size
                if pid != self.pid:
                    self.remote[pid] = [x, y, vx, vy, time.monotonic()]
            present = self._ids(payload, count)
            for pid in [p for p in self.remote if p not in present]:
                del self.remote[pid]
        elif seq == self.snap_last:
            self.snap_dup += 1
        else:
            self.snap_stale += 1                                     # old state: ignore
            if self.snap_lost > 0:
                self.snap_lost -= 1

    @staticmethod
    def _ids(payload, count):
        return {SNAP_ENTRY.unpack_from(payload, SNAP_HEAD.size + i * SNAP_ENTRY.size)[0] for i in range(count)}

    def on_datagram(self, data):
        m = unpack(data)
        if not m:
            return
        t, f, pid, seq, ts, payload = m
        if t == T_SNAPSHOT and len(payload) >= SNAP_HEAD.size:
            self.on_snapshot(seq, ts, payload)
        elif t == T_JOIN_REJECT and f == REJ_NOT_REGISTERED:
            self.need_rejoin = True

    # ----------------------------------------------------------------- sending
    def tick(self, dt):
        # random-walk bot with wall bouncing
        if random.random() < 0.05:
            self.vx += random.uniform(-30, 30)
            self.vy += random.uniform(-30, 30)
        self.x += self.vx * dt
        self.y += self.vy * dt
        if not 0 <= self.x <= ARENA:
            self.vx, self.x = -self.vx, min(max(self.x, 0), ARENA)
        if not 0 <= self.y <= ARENA:
            self.vy, self.y = -self.vy, min(max(self.y, 0), ARENA)
        self.seq += 1
        self.send(T_STATE, self.seq, STATE_PAYLOAD.pack(self.x, self.y, self.vx, self.vy))
        self.sent += 1
        # dead reckoning for remote players whose updates are late/lost
        now = time.monotonic()
        for pid, (x, y, vx, vy, t) in self.remote.items():
            age = now - t
            if age > 1.5 * self.snap_period:
                self.extrap_ticks += 1
                self.view[pid] = (x + vx * age, y + vy * age)
            else:
                self.view[pid] = (x, y)

    def report(self):
        expected = self.snap_rx + self.snap_lost
        return {
            "name": self.name, "pid": self.pid, "state_sent": self.sent,
            "snapshots_rx": self.snap_rx, "snapshots_expected": expected, "snapshots_lost": self.snap_lost,
            "loss_pct": round(100.0 * self.snap_lost / expected, 2) if expected else 0.0,
            "duplicates": self.snap_dup, "stale_discarded": self.snap_stale,
            "rtt_ms": summarize(self.rtt), "owd_down_ms": summarize(self.owd_down),
            "jitter_rfc3550_ms": round(self.jitter.rfc_ms, 3), "jitter_mean_ms": round(self.jitter.mean_ms, 3),
            "extrapolated_ticks": self.extrap_ticks, "join_retries": self.join_retries, "rejoins": self.rejoins}

    def run(self, duration):
        signal.signal(signal.SIGINT, lambda *_: setattr(self, "running", False))
        signal.signal(signal.SIGTERM, lambda *_: setattr(self, "running", False))
        self.need_rejoin = False
        if not self.register():
            print("[%s] registration failed (server full or unreachable)" % self.name)
            return None
        if not self.quiet:
            print("[%s] registered as player %d" % (self.name, self.pid), flush=True)
        period = 1.0 / self.hz
        start = time.monotonic()
        next_send, next_print = start, start + 1
        while self.running and time.monotonic() - start < duration:
            select.select([self.sock], [], [], max(0.0, min(next_send - time.monotonic(), 0.05)))
            while True:
                try:
                    data, _ = self.sock.recvfrom(2048)
                except (BlockingIOError, ConnectionResetError):
                    break
                self.on_datagram(data)
            now = time.monotonic()
            if self.need_rejoin or now - self.last_snap_rx > 3.0:   # server forgot us / went silent
                self.need_rejoin = False
                self.rejoins += 1
                if not self.quiet:
                    print("[%s] re-registering..." % self.name, flush=True)
                self.register()
                continue
            if now >= next_send:
                self.tick(period)
                next_send += period
                if now - next_send > 1.0:
                    next_send = now + period
            if not self.quiet and now >= next_print:
                next_print += 1
                r = self.rtt[-1] / 1000.0 if self.rtt else 0
                print("[%s] t=%2ds players_seen=%d rtt=%.1fms jitter=%.2fms snap_loss=%d" % (
                    self.name, now - start, len(self.remote) + 1, r, self.jitter.rfc_ms, self.snap_lost), flush=True)
        self.leave()
        return self.report()


def main():
    ap = argparse.ArgumentParser(description="UDP game client (bot)")
    ap.add_argument("--server", default="127.0.0.1:%d" % GAME_PORT, help="host:port")
    ap.add_argument("--name", default="player%d" % random.randint(100, 999))
    ap.add_argument("--hz", type=int, default=20, help="state updates per second")
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--dscp", type=int, default=DSCP_EF, help="DSCP marking (0 = off)")
    ap.add_argument("--out", help="write JSON report here")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    c = GameClient(a.server, a.name, a.hz, a.dscp, a.quiet)
    rep = c.run(a.duration)
    if rep is None:
        raise SystemExit(1)
    if a.out:
        with open(a.out, "w") as f:
            json.dump(rep, f, indent=2)
    if not a.quiet:
        print(json.dumps(rep, indent=2))


if __name__ == "__main__":
    main()
