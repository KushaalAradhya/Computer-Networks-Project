"""Ryu (OpenFlow 1.3) controller: learning switch + dynamic prioritization of UDP game traffic.

Run:  ryu-manager game_qos_controller.py
Mode (file /tmp/game_qos_mode, polled every second):  on | off | auto
  on   : game packets (UDP port 5000) are sent to OVS queue 1 (high priority)
  off  : everything uses queue 0 (plain forwarding)
  auto : prioritization turns on when any switch port exceeds CONGEST_THRESHOLD of
         CAPACITY_BPS and off again when it falls below half the threshold
Env: GAME_QOS_MODE_FILE, CAPACITY_BPS (default 10e6), CONGEST_THRESHOLD (default 0.8)
"""
import json
import os
import time

from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER, DEAD_DISPATCHER, set_ev_cls
from ryu.lib import hub
from ryu.lib.packet import ethernet, ether_types, ipv4, packet, udp
from ryu.ofproto import ofproto_v1_3

GAME_PORT, GAME_QUEUE = 5000, 1
MODE_FILE = os.environ.get("GAME_QOS_MODE_FILE", "/tmp/game_qos_mode")
CAPACITY = float(os.environ.get("CAPACITY_BPS", 10e6))
THRESH = float(os.environ.get("CONGEST_THRESHOLD", 0.8))
EVENT_LOG = "/tmp/game_qos_events.log"


class GameQoS(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.mac_to_port, self.dps, self.last_bytes = {}, {}, {}
        self.mode, self.prio_active = "off", False
        hub.spawn(self._mode_loop)
        hub.spawn(self._stats_loop)

    def _event(self, msg):
        line = json.dumps({"t": time.time(), "event": msg})
        self.logger.info(line)
        with open(EVENT_LOG, "a") as f:
            f.write(line + "\n")

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def features_handler(self, ev):
        dp = ev.msg.datapath
        self._table_miss(dp)

    def _table_miss(self, dp):
        p = dp.ofproto_parser
        self._add_flow(dp, 0, p.OFPMatch(), [p.OFPActionOutput(dp.ofproto.OFPP_CONTROLLER, dp.ofproto.OFPCML_NO_BUFFER)])

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def state_change(self, ev):
        if ev.state == MAIN_DISPATCHER:
            self.dps[ev.datapath.id] = ev.datapath
        else:
            self.dps.pop(ev.datapath.id, None)

    def _add_flow(self, dp, prio, match, actions, idle=0):
        p, o = dp.ofproto_parser, dp.ofproto
        inst = [p.OFPInstructionActions(o.OFPIT_APPLY_ACTIONS, actions)]
        dp.send_msg(p.OFPFlowMod(datapath=dp, priority=prio, match=match, instructions=inst, idle_timeout=idle))

    def _flush(self):
        for dp in self.dps.values():
            o, p = dp.ofproto, dp.ofproto_parser
            dp.send_msg(p.OFPFlowMod(datapath=dp, command=o.OFPFC_DELETE, out_port=o.OFPP_ANY,
                                     out_group=o.OFPG_ANY, match=p.OFPMatch()))
            self._table_miss(dp)

    def _set_prio(self, active, why):
        if active != self.prio_active:
            self.prio_active = active
            self._event("priority %s (%s)" % ("ENABLED" if active else "DISABLED", why))
            self._flush()                      # flows are re-learned with / without the queue action

    def _mode_loop(self):
        while True:
            try:
                mode = open(MODE_FILE).read().strip().lower()
            except OSError:
                mode = "off"
            if mode in ("on", "off", "auto") and mode != self.mode:
                self.mode = mode
                self._event("mode -> " + mode)
                if mode != "auto":
                    self._set_prio(mode == "on", "manual")
            hub.sleep(1)

    def _stats_loop(self):
        while True:
            for dp in list(self.dps.values()):
                dp.send_msg(dp.ofproto_parser.OFPPortStatsRequest(dp, 0, dp.ofproto.OFPP_ANY))
            hub.sleep(1)

    @set_ev_cls(ofp_event.EventOFPPortStatsReply, MAIN_DISPATCHER)
    def port_stats(self, ev):
        now, peak = time.time(), 0.0
        for s in ev.msg.body:
            key = (ev.msg.datapath.id, s.port_no)
            if key in self.last_bytes:
                b0, t0 = self.last_bytes[key]
                if now > t0:
                    peak = max(peak, (s.tx_bytes - b0) * 8 / (now - t0))
            self.last_bytes[key] = (s.tx_bytes, now)
        if self.mode == "auto":
            if peak > THRESH * CAPACITY:
                self._set_prio(True, "congestion %.1f Mbps" % (peak / 1e6))
            elif peak < THRESH * CAPACITY / 2:
                self._set_prio(False, "congestion cleared")

    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in(self, ev):
        msg, dp = ev.msg, ev.msg.datapath
        o, p = dp.ofproto, dp.ofproto_parser
        in_port = msg.match["in_port"]
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)
        if eth.ethertype == ether_types.ETH_TYPE_LLDP:
            return
        table = self.mac_to_port.setdefault(dp.id, {})
        table[eth.src] = in_port
        out = table.get(eth.dst, o.OFPP_FLOOD)
        if out != o.OFPP_FLOOD:
            # plain L2 rule (queue 0)
            self._add_flow(dp, 1, p.OFPMatch(eth_dst=eth.dst), [p.OFPActionOutput(out)], idle=60)
            if self.prio_active:               # game rules: UDP port 5000 in either direction -> queue 1
                acts = [p.OFPActionSetQueue(GAME_QUEUE), p.OFPActionOutput(out)]
                for f in ({"udp_dst": GAME_PORT}, {"udp_src": GAME_PORT}):
                    self._add_flow(dp, 100, p.OFPMatch(eth_dst=eth.dst, eth_type=0x0800, ip_proto=17, **f), acts, idle=60)
        acts = [p.OFPActionOutput(out)]
        u = pkt.get_protocol(udp.udp)
        if self.prio_active and u and GAME_PORT in (u.src_port, u.dst_port) and out != o.OFPP_FLOOD:
            acts = [p.OFPActionSetQueue(GAME_QUEUE), p.OFPActionOutput(out)]
        data = msg.data if msg.buffer_id == o.OFP_NO_BUFFER else None
        dp.send_msg(p.OFPPacketOut(datapath=dp, buffer_id=msg.buffer_id, in_port=in_port, actions=acts, data=data))
