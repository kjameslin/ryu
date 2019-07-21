from collections import defaultdict
from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.topology import event
from ryu.controller.handler import MAIN_DISPATCHER,CONFIG_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet
from ryu.lib.packet import ethernet
from ryu.lib.packet import ether_types
#https://github.com/Ehsan70/RyuApps/blob/master/TopoDiscoveryInRyu.md
from ryu.topology.api import get_switch,get_all_link,get_link
import copy

from ryu.lib.packet import arp
from ryu.lib.packet import ipv6
from ryu.lib import mac



# this is topo implementing dijkstra algorithm
class Topo(object):
    def __init__(self):
        # adjacent map (s1,s2)->(port,weight)
        self.adjacent=defaultdict(lambda s1s2:None)
        #datapathes
        self.switches=None
        # switch distances

        # use a map to host_mac->(switch,inport)
        self.host_mac_to={}
    
    def reset(self):
        self.adjacent=defaultdict(lambda s1s2:None)
        self.switches=None
        self.host_mac_to=None
    
    
    def get_adjacent(self,s1,s2):
        return self.adjacent.get((s1,s2))
    
    def set_adjacent(self,s1,s2,port,weight):
        self.adjacent[(s1,s2)]=(port,weight)
    
    #find the switch with min distance
    def __min_dist(self,distances, Q):
        mm=float('Inf')

        m_node=None
        for v in Q:
            if distances[v]<=mm:
                mm=distances[v]
                m_node=v
        return m_node
    
    #src 
    #https://en.wikipedia.org/wiki/Dijkstra%27s_algorithm
    def shortest_path(self,src_sw,dst_sw,first_port,last_port):
        
        distance={}
        previous={}

        #dpid is logical switch
        # init

        assert self.switches is not None
        for dpid in self.switches:
            distance[dpid]=float('Inf')
            previous[dpid]=None
        
        distance[src_sw]=0
        Q=set(self.switches)

        while len(Q)>0:
            u=self.__min_dist(distance,Q)
            Q.remove(u)

            for s in self.switches:
                # get u->s port weight
                # for each neighbor s of u:   
                if self.get_adjacent(u,s) is not None:
                    _,weight=self.get_adjacent(u,s)
                    if distance[u]+weight<=distance[s]:
                        distance[s]=distance[u]+weight
                        previous[s]=u

            # record path

        record=[]
            
        record.append(dst_sw)
        q=previous[dst_sw]

        while q is not None:
            if q==src_sw:
                    #we find it
                record.append(q)
                break
            p=q
            record.append(p)
            q=previous[p]
            
        #we reverse the list 
        # src-s-s-s-s-dst

        record.reverse()

        if src_sw==dst_sw:
                path=[src_sw]
        else:
                path=record
            
        record=[]
        inport=first_port

            # s1 s2; s2:s3, sn-1  sn
        for s1,s2 in zip(path[:-1],path[1:]):
                # s1--outport-->s2
            outport,_=self.get_adjacent(s1,s2)
                
            record.append((s1,inport,outport))
            inport,_=self.get_adjacent(s2,s1)
            
        record.append((dst_sw,inport,last_port))
        
            #inport s1 outport----inport s2 outport---...--inport sn lastport
        return record


#TODO Port status monitor
class DijkstraController(app_manager.RyuApp):
    OFP_VERSIONS=[ofproto_v1_3.OFP_VERSION]

    def __init__(self,*args,**kwargs):
        super(DijkstraController,self).__init__(*args,**kwargs)
        self.mac_to_port={}
        # logical switches
        self.datapaths=[]
        #ip ->mac
        self.arp_table={}
        #revser arp
        # mac->ip
        self.rarp_table={}

        self.topo=Topo()
        #avoid broadcast storm
        # {
        #   dpid:[]
        # }
        #
        self.flood_history={}
        self.sw={}
    
    def _find_dp(self,dpid):
        for dp in self.datapaths:
            if dp.id==dpid:
                return dp
        return None

    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        # install table-miss flow entry
        #
        # We specify NO BUFFER to max_len of the output action due to
        # OVS bug. At this moment, if we specify a lesser number, e.g.,
        # 128, OVS will send Packet-In with invalid buffer_id and
        # truncated packet data. In that case, we cannot output packets
        # correctly.  The bug has been fixed in OVS v2.1.0.
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]
        self.add_flow(datapath, 0, match, actions)
    
    def add_flow(self, datapath, priority, match, actions, buffer_id=None):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]
        if buffer_id:
            mod = parser.OFPFlowMod(datapath=datapath, buffer_id=buffer_id,
                                    priority=priority, match=match,
                                    instructions=inst)
        else:
            mod = parser.OFPFlowMod(datapath=datapath, priority=priority,
                                    match=match, instructions=inst)
        datapath.send_msg(mod)


    def configure_path(self,shortest_path:list,event,src_mac,dst_mac):
        #configure shortest path to switches
        msg=event.msg
        datapath=msg.datapath

        ofproto=datapath.ofproto

        parser=datapath.ofproto_parser

        #inport s1 outport----inport s2 outport---...--inport sn lastport
        for switch,inport,outport in shortest_path:
            match=parser.OFPMatch(in_port=inport,eth_src=src_mac,eth_dst=dst_mac)

            actions=[parser.OFPActionOutput(outport)]

            # get current switch,index minus 1
            datapath=self._find_dp(int(switch))
            # datapath=self.datapaths[int(switch)-1]

            inst=[parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,actions)]

            mod=datapath.ofproto_parser.OFPFlowMod(
                datapath=datapath,
                match=match,
                idle_timeout=0,
                hard_timeout=0,
                priority=1,
                instructions=inst
            )
            datapath.send_msg(mod)

    
    @set_ev_cls(ofp_event.EventOFPPacketIn,MAIN_DISPATCHER)
    def packet_in_handler(self,event):

        msg=event.msg
        datapath=msg.datapath
        ofproto=datapath.ofproto
        parser=datapath.ofproto_parser

        in_port=msg.match['in_port']

        self.logger.info("From datapath {} port {} come in a packet".format(datapath.id,in_port))

        #get src_mac and dist mac
        pkt=packet.Packet(msg.data)
        eth=pkt.get_protocols(ethernet.ethernet)[0]

        # drop lldp
        if eth.ethertype==ether_types.ETH_TYPE_LLDP:
            self.logger.info("LLDP")
            return

        dst_mac=eth.dst
        src_mac=eth.src
        arp_pkt = pkt.get_protocol(arp.arp)
        if arp_pkt:
            self.arp_table[arp_pkt.src_ip] = src_mac  # ARP learning

        dpid=datapath.id

        self.mac_to_port.setdefault(dpid,{})

        self.flood_history.setdefault(dpid,[])
        if '33:33' in dst_mac[:5]:
            if (src_mac,dst_mac) not in self.flood_history[dpid]:
                self.flood_history[dpid].append((src_mac,dst_mac))
            else:
                return
                
        self.logger.info("packet in %s %s %s %s", dpid, src_mac, dst_mac, in_port)

        ''' 
        we record mac address - port
        mac_to_port:{
            dpid:{
                src_mac:in_port
            }
        }
        '''

        self.mac_to_port[dpid][src_mac]=in_port
        flood=False

        # if '33:33' not in dst_mac[:5]:
        #     flood=False
        # if '33:33' in dst_mac[:5] and dst_mac in self.flood_history[dpid]:
        #     flood=False
        
        # if '33:33' in dst_mac[:5] and dst_mac not in self.flood_history[dpid]:
        #     flood=True
        

        if src_mac not in self.topo.host_mac_to.keys():
            self.topo.host_mac_to[src_mac]=(dpid,in_port)
        
        # we must assure all the mac has registered
        if dst_mac in self.topo.host_mac_to.keys():
            # the dst mac has registered
            final_port=self.topo.host_mac_to[dst_mac][1]
            # calculate the first
            src_switch=self.topo.host_mac_to[src_mac][0]
            dst_switch=self.topo.host_mac_to[dst_mac][0]
            shortest_path=self.topo.shortest_path(
                src_switch,
                dst_switch,
                in_port,
                final_port)
            
            self.logger.info("The shortest path from {} to {} contains {} switches".format(src_mac,dst_mac,len(shortest_path)))
            
            assert len(shortest_path)>0
            
            # log the shortest path
            path_str=''

            #inport s1 outport----inport s2 outport---...--inport sn lastport
            for s,ip,op in shortest_path:
                path_str=path_str+"--{}-{}-{}--".format(ip,s,op)
            self.logger.info("The shortest path from {} to {} is {}".format(src_mac,dst_mac,path_str))
            
            self.logger.info("Have calculated the shortest path from {} to {}".format(src_mac,dst_mac))
            self.logger.info("Now configuring switches of interest")
            self.configure_path(shortest_path,event,src_mac,dst_mac)
            self.logger.info("Configure done")

            out_port=shortest_path[0][2]
        else: 
            if self.arp_handler(msg):  # 1:reply or drop;  0: flood
                return 
            #the dst mac has not registered
            out_port=ofproto.OFPP_FLOOD

        # if out_port != ofproto.OFPP_FLOOD:
        #     self.add_flow(datapath, msg.in_port, dst, src, actions)

        actions=[parser.OFPActionOutput(out_port)]

        data=None

        if msg.buffer_id==ofproto.OFP_NO_BUFFER:
            data=msg.data
        
        out=parser.OFPPacketOut(
            datapath=datapath,
            buffer_id=msg.buffer_id,
            in_port=in_port,
            actions=actions,
            data=data
        )
        datapath.send_msg(out)
        
    #https://vlkan.com/blog/post/2013/08/06/sdn-discovery/
    @set_ev_cls(event.EventSwitchEnter)
    def switch_enter_handler(self,event):
        self.logger.info("A switch entered.Topology rediscovery...")
        self.switch_status_handler(event)
        self.logger.info('Topology rediscovery done')
    
    @set_ev_cls(event.EventSwitchLeave)
    def switch_leave_handler(self,event):
        self.logger.info("A switch leaved.Topology rediscovery...")
        self.switch_status_handler(event)
        self.logger.info('Topology rediscovery done')



    def switch_status_handler(self,event):
        #api get_switch
        #api app.send_request()
        #api switch_request_handler
        #return reply.switches
        # switch.dp.id
        all_switches=copy.copy(get_switch(self,None))

        # get all datapathid 
        self.topo.switches=[s.dp.id for s in all_switches]

        self.logger.info("switches {}".format(self.topo.switches))

        self.datapaths=[s.dp for s in all_switches]

        # get link and get port
        all_links=copy.copy(get_link(self,None))
        #api link_request_handler
        #api Link
        # link port 1,port 2

        all_link_stats=[(l.src.dpid,l.dst.dpid,l.src.port_no,l.dst.port_no) for l in all_links]
        self.logger.info("Number of links {}".format(len(all_link_stats)))

        all_link_repr=''

        for s1,s2,p1,p2 in all_link_stats:
            all_link_repr+='s{}p{}--s{}p{}\n'.format(s1,p1,s2,p2)
            # we would assign weight randomly
            # but we have to consider the weight consistency
            self.topo.set_adjacent(s1,s2,p1,1)
            self.topo.set_adjacent(s2,s1,p2,1)
        self.logger.info("All links:\n "+all_link_repr)
    
    #https://github.com/osrg/ryu/pull/55/commits/8916ab85072efc75b97f987a0696ff1fe64cbf42
    # reference 
    # packet api https://ryu.readthedocs.io/en/latest/library_packet.html
    # arppacket api https://ryu.readthedocs.io/en/latest/library_packet_ref/packet_arp.html
    #TODO figure out how the function works
    def arp_handler(self, msg):
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser
        in_port = msg.match['in_port']

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocols(ethernet.ethernet)[0]
        arp_pkt = pkt.get_protocol(arp.arp)

        if eth:
            eth_dst = eth.dst
            eth_src = eth.src

         # Break the loop for avoiding ARP broadcast storm
        if eth_dst == mac.BROADCAST_STR and arp_pkt:
            arp_dst_ip = arp_pkt.dst_ip

            if (datapath.id, eth_src, arp_dst_ip) in self.sw:
                if self.sw[(datapath.id, eth_src, arp_dst_ip)] != in_port:
                    datapath.send_packet_out(in_port=in_port, actions=[])
                    return True
            else:
                self.sw[(datapath.id, eth_src, arp_dst_ip)] = in_port

         # Try to reply arp request
        if arp_pkt:
            hwtype = arp_pkt.hwtype
            proto = arp_pkt.proto
            hlen = arp_pkt.hlen
            plen = arp_pkt.plen
            opcode = arp_pkt.opcode
            arp_src_ip = arp_pkt.src_ip
            arp_dst_ip = arp_pkt.dst_ip

            if opcode == arp.ARP_REQUEST:
                if arp_dst_ip in self.arp_table:
                    actions = [parser.OFPActionOutput(in_port)]
                    ARP_Reply = packet.Packet()

                    ARP_Reply.add_protocol(ethernet.ethernet(
                        ethertype=eth.ethertype,
                        dst=eth_src,
                        src=self.arp_table[arp_dst_ip]))
                    ARP_Reply.add_protocol(arp.arp(
                        opcode=arp.ARP_REPLY,
                        src_mac=self.arp_table[arp_dst_ip],
                        src_ip=arp_dst_ip,
                        dst_mac=eth_src,
                        dst_ip=arp_src_ip))

                    ARP_Reply.serialize()

                    out = parser.OFPPacketOut(
                        datapath=datapath,
                        buffer_id=ofproto.OFP_NO_BUFFER,
                        in_port=ofproto.OFPP_CONTROLLER,
                        actions=actions, data=ARP_Reply.data)
                    datapath.send_msg(out)
                    return True
        return False


