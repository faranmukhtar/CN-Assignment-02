import random
import socket
import time
import threading
from Q3_COMMON import Packet , UnreliableChannel , log , SENDER_ADDR , RECEIVER_ADDR , CHANNEL_ADDR
# MY ROLE NUMBER IS 24k-0646 so seed is set to 646
ROLL = 646

class GBNSender:
    def __init__(self, num_packets, N, k, timeout_sec):
        self.num_packets = num_packets
        self.N = N  
        self.k = k  
        self.mod = 2 ** k
        self.timeout_sec = timeout_sec
        self.base = 0
        self.nextseqnum = 0
        self.packet_idx = 0
        self.total_acked = 0
        self.retransmit = 0
        self.packets = [Packet(seq_num=i % self.mod, payload=f"Data_{i}") for i in range(num_packets)]
        self.lock = threading.Lock()
        self.timer = None
        self.running = False
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(SENDER_ADDR)
        self.sock.settimeout(0.1)
        self.transmission = 0

    def send_packet(self, pkt):
        self.sock.sendto(pkt.to_bytes(), CHANNEL_ADDR)

    def start_timer(self):
        self.stop_timer()
        self.timer = threading.Timer(self.timeout_sec, self.on_timeout)
        self.timer.start()

    def stop_timer(self):
        if self.timer:
            self.timer.cancel()
            self.timer = None

    def on_timeout(self):
        with self.lock:
            if self.total_acked >= self.num_packets:
                return
            log(f"[TIMEOUT] GBN Timer expired for oldest unACKed packet (base={self.base})")
            self.start_timer()
            for idx in range(self.total_acked, self.packet_idx):
                pkt = self.packets[idx]
                log(f"[RETRANSMIT] GBN Resending Packet (seq={pkt.seq_num}, payload='{pkt.payload}')")
                self.retransmit = self.retransmit+1
                self.transmission = self.transmission +1
                self.send_packet(pkt)

    def run(self):
        self.running = True
        ack_thread = threading.Thread(target=self._listen_acks, daemon=True)
        ack_thread.start()

        while self.total_acked < self.num_packets:
            with self.lock:
                if (self.packet_idx - self.total_acked < self.N) and (self.packet_idx < self.num_packets):
                    pkt = self.packets[self.packet_idx]
                    log(f"[SEND] GBN Sender sent Packet (seq={pkt.seq_num}, payload='{pkt.payload}')")
                    self.transmission = self.transmission +1

                    if self.total_acked == self.packet_idx:
                        self.start_timer()
                    self.packet_idx += 1
                    self.nextseqnum = self.packet_idx % self.mod
                    self.send_packet(pkt)
            time.sleep(0.01)

        self.stop_timer()
        self.running = False
        ack_thread.join()

    def _listen_acks(self):
        while self.running or (self.total_acked < self.num_packets):
            try:
                data, _ = self.sock.recvfrom(2048)
                ack_packet = Packet.from_bytes(data)
                if not ack_packet.verify_checksum():
                    log(f"[CORRUPT DISCARD] GBN Sender discarded corrupted ACK")
                    continue

                log(f"[ACK RECEIVE] GBN Sender received Cumulative ACK (ack={ack_packet.ack_num})")
                with self.lock:
                    match_idx = -1
                    for idx in range(self.total_acked, min(self.total_acked + self.N, self.num_packets)):
                        if self.packets[idx].seq_num == ack_packet.ack_num:
                            match_idx = idx

                    if match_idx != -1:
                        new_acked = match_idx + 1
                        if new_acked > self.total_acked:
                            self.total_acked = new_acked
                            if self.total_acked < self.num_packets:
                                self.base = self.packets[self.total_acked].seq_num
                            else:
                                self.base = (self.packets[-1].seq_num + 1) % self.mod

                            if self.total_acked == self.packet_idx:
                                self.stop_timer()
                            else:
                                self.start_timer()
            except socket.timeout:
                continue


class GBNReceiver:
    def __init__(self, num_packets, k):
        self.num_packets = num_packets
        self.k = k
        self.mod = 2 ** k
        self.expectedseqnum = 0
        self.delivered_count = 0
        self.finished = False

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(RECEIVER_ADDR)
        self.sock.settimeout(0.1)

    def send_ack(self, ack):
        self.sock.sendto(ack.to_bytes(), CHANNEL_ADDR)

    def run(self):
        while not self.finished:
            try:
                data, _ = self.sock.recvfrom(2048)
                pkt = Packet.from_bytes(data)
                if not pkt.verify_checksum():
                    log(f"[CORRUPT DISCARD] GBN Receiver discarded corrupted Packet (seq={pkt.seq_num})")
                    last_ack = (self.expectedseqnum - 1) % self.mod
                    self.send_ack(Packet(ack_num=last_ack, is_ack=True))
                    continue

                log(f"[RECEIVE] GBN Receiver got Packet (seq={pkt.seq_num}, payload='{pkt.payload}')")
                if pkt.seq_num == self.expectedseqnum:
                    log(f"[DELIVER] GBN Receiver delivered Packet (seq={pkt.seq_num}) to application")
                    ack = Packet(ack_num=self.expectedseqnum, is_ack=True)
                    log(f"[ACK SEND] GBN Receiver sending Cumulative ACK (ack={self.expectedseqnum})")
                    self.expectedseqnum = (self.expectedseqnum + 1) % self.mod
                    self.delivered_count += 1
                    self.send_ack(ack)
                else:
                    log(f"[DISCARD] GBN Receiver discarded out-of-order Packet (seq={pkt.seq_num}, expected={self.expectedseqnum})")
                    last_ack = (self.expectedseqnum - 1) % self.mod
                    log(f"[ACK SEND] GBN Receiver re-sending duplicate ACK (ack={last_ack})")
                    self.send_ack(Packet(ack_num=last_ack, is_ack=True))
            except socket.timeout:
                continue

print("GBN Implementation Faran Mukhtar - 24k-0646")
num_packets= int(input("Enter number of packets: "))
N= int(input ("Enter Window size(N): "))
k= int(input ("Enter Sequence number bits(K): "))



loss_prob=0.05
corrupt_prob=0.05
delay_prob=0.1
delay_sec=0.2
timeout_sec=0.8


print("=" * 80)
print(f"RUNNING GBN SIMULATION | Packets: {num_packets} | Window N: {N} | k-bits: {k} (mod {2**k})")
print(f"Channel Errors -> Loss: {loss_prob} | Corrupt: {corrupt_prob} | Delay: {delay_prob} ({delay_sec}s) | Timeout: {timeout_sec}s")
print("=" * 80)
random.seed(ROLL)
channel = UnreliableChannel(loss_prob, corrupt_prob, delay_prob, delay_sec)
sender = GBNSender(num_packets, N, k, timeout_sec)
receiver = GBNReceiver(num_packets, k)

chan_thread = threading.Thread(target=channel.run)
rec_thread = threading.Thread(target=receiver.run)
snd_thread = threading.Thread(target=sender.run)

start = time.perf_counter()

chan_thread.start()
rec_thread.start()
snd_thread.start()

snd_thread.join()
receiver.finished = True
rec_thread.join()
channel.running = False
chan_thread.join()

elapsed = time.perf_counter() - start
sender.sock.close()
receiver.sock.close()
channel.sock.close()
print("=" * 80)
print(f"SUCCESS: All {num_packets} packets transmitted and delivered!")
print(f"Transmissions: {sender.transmission} , Retransmissions : {sender.retransmit}")

print(f"Time elapsed: {elapsed}")

print("=" * 80 + "\n")