"""Shared protocol definitions and helpers for the UDP multiplayer game network.

Wire format (network byte order)
--------------------------------
Header (18 bytes): magic(2)='GM' | type(1) | flags(1) | player_id(2) | seq(4) | timestamp_us(8)

  JOIN        client->server  payload: name(16s)               reliable (client retries)
  JOIN_ACK    server->client  payload: tick_hz(B)
  JOIN_REJECT server->client  flags: 1 = not registered (rejoin), 2 = server full
  STATE       client->server  payload: x,y,vx,vy (4 floats)    seq = per-player counter
  SNAPSHOT    server->client  payload: echo_seq(I) echo_ts(Q) hold_us(I) count(B)
                                       + count * [pid(H) x y vx vy]   seq = global snapshot counter
  LEAVE       client->server  reliable (client retries)
  LEAVE_ACK   server->client
"""
import math
import socket
import struct
import time

MAGIC = b"GM"
HDR = struct.Struct("!2sBBHIQ")
T_JOIN, T_JOIN_ACK, T_JOIN_REJECT, T_STATE, T_SNAPSHOT, T_LEAVE, T_LEAVE_ACK = range(1, 8)
REJ_NOT_REGISTERED = 1
REJ_FULL = 2

JOIN_PAYLOAD = struct.Struct("!16s")
ACK_PAYLOAD = struct.Struct("!B")
STATE_PAYLOAD = struct.Struct("!ffff")
SNAP_HEAD = struct.Struct("!IQIB")
SNAP_ENTRY = struct.Struct("!Hffff")

GAME_PORT = 5000       # UDP port the SDN controller classifies as "game traffic"
NOISE_PORT = 6000      # background/congestion traffic
DSCP_EF = 46           # Expedited Forwarding: used to mark game packets
ARENA = 1000.0


def now_us():
    """Wall-clock microseconds (hosts in Mininet / one machine share a clock)."""
    return time.time_ns() // 1000


def mark_dscp(sock, dscp):
    try:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, dscp << 2)
    except OSError:
        pass


def pack(msg_type, pid, seq, payload=b"", flags=0, ts=None):
    return HDR.pack(MAGIC, msg_type, flags, pid, seq & 0xFFFFFFFF,
                    now_us() if ts is None else ts) + payload


def unpack(data):
    """Returns (type, flags, pid, seq, ts_us, payload) or None if malformed."""
    if len(data) < HDR.size:
        return None
    magic, t, f, pid, seq, ts = HDR.unpack_from(data)
    if magic != MAGIC:
        return None
    return t, f, pid, seq, ts, data[HDR.size:]


class Jitter:
    """RFC 3550 interarrival jitter plus mean |delay variation| (IPDV)."""

    def __init__(self):
        self.prev_transit = None
        self.rfc = 0.0       # microseconds
        self.sum_d = 0.0
        self.n = 0

    def update(self, send_ts_us, recv_ts_us):
        transit = recv_ts_us - send_ts_us
        if self.prev_transit is not None:
            d = abs(transit - self.prev_transit)
            self.rfc += (d - self.rfc) / 16.0
            self.sum_d += d
            self.n += 1
        self.prev_transit = transit

    @property
    def rfc_ms(self):
        return self.rfc / 1000.0

    @property
    def mean_ms(self):
        return (self.sum_d / self.n / 1000.0) if self.n else 0.0


def summarize(samples_us):
    """Summary statistics in milliseconds for a list of microsecond samples."""
    if not samples_us:
        return {"n": 0, "mean": 0, "min": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0}
    s = sorted(samples_us)
    n = len(s)

    def pct(p):
        return s[min(n - 1, int(math.ceil(p / 100.0 * n)) - 1)] / 1000.0

    return {"n": n, "mean": round(sum(s) / n / 1000.0, 3), "min": round(s[0] / 1000.0, 3),
            "p50": round(pct(50), 3), "p95": round(pct(95), 3),
            "p99": round(pct(99), 3), "max": round(s[-1] / 1000.0, 3)}
