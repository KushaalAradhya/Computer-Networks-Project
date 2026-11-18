#!/usr/bin/env python3
"""Background UDP traffic generator / sink used to create controlled congestion.

  sink : python3 traffic_gen.py sink --port 6000
  send : python3 traffic_gen.py send --dst 10.0.0.6 --port 6000 --mbps 12 --duration 20
"""
import argparse
import json
import socket
import time

from game_common import NOISE_PORT, mark_dscp


def send(a):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    mark_dscp(s, a.dscp)
    payload = bytes(a.size)
    pps = a.mbps * 1e6 / 8 / a.size
    start = time.monotonic()
    sent = 0
    while True:
        el = time.monotonic() - start
        if el >= a.duration:
            break
        due = int(el * pps) - sent
        for _ in range(min(due, 500)):
            try:
                s.sendto(payload, (a.dst, a.port))
            except OSError:
                pass
            sent += 1
        time.sleep(0.001)
    print(json.dumps({"role": "sender", "packets": sent, "mbps_offered": round(sent * a.size * 8 / a.duration / 1e6, 3)}))


def sink(a):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("0.0.0.0", a.port))
    s.settimeout(0.5)
    pk = by = 0
    start = last = time.monotonic()
    while a.duration <= 0 or time.monotonic() - start < a.duration:
        try:
            d, _ = s.recvfrom(65535)
            pk += 1
            by += len(d)
        except socket.timeout:
            pass
        if time.monotonic() - last >= 1:
            last = time.monotonic()
            print("[sink] %d pkts, %.2f Mbps avg" % (pk, by * 8 / (last - start) / 1e6), flush=True)
    print(json.dumps({"role": "sink", "packets": pk, "mbps": round(by * 8 / (time.monotonic() - start) / 1e6, 3)}))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["send", "sink"])
    ap.add_argument("--dst", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=NOISE_PORT)
    ap.add_argument("--mbps", type=float, default=10.0)
    ap.add_argument("--size", type=int, default=1200)
    ap.add_argument("--duration", type=float, default=20)
    ap.add_argument("--dscp", type=int, default=0)
    a = ap.parse_args()
    (send if a.mode == "send" else sink)(a)
