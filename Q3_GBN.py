import random
import time
import threading
from queue import Queue, Empty
from Q3_COMMON import Packet , UnreliableChannel
# MY ROLE NUMBER IS 24k-0646 so seed is set to 646
ROLL = 646

import time

start = time.perf_counter()

class GBNSender:
    def __init__(self, num_packets, N, k, timeout_sec, channel):
        self.num_packets = num_packets
        self.N = N  
        self.k = k 
        self.mod = 2 ** k
        self.timeout_sec = timeout_sec
        self.channel = channel

        self.base = 0
        self.nextseqnum = 0
        self.packet_idx = 0
        self.total_acked = 0

        self.packets = [Packet(seq_num=i % self.mod, payload=f"Data_{i}") for i in range(num_packets)]
        self.lock = threading.Lock()
        self.timer = None
        self.running = False

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
            self.channel.log(f"[TIMEOUT] GBN Timer expired for oldest unACKed packet (base={self.base})")
            self.start_timer()
            for idx in range(self.total_acked, self.packet_idx):
                pkt = self.packets[idx]
                self.channel.log(f"[RETRANSMIT] GBN Resending Packet (seq={pkt.seq_num}, payload='{pkt.payload}')")
                self.channel.send_to_receiver(pkt)

    def run(self):
        self.running = True
        ack_thread = threading.Thread(target=self._listen_acks, daemon=True)
        ack_thread.start()

        while self.total_acked < self.num_packets:
            with self.lock:
                if (self.packet_idx - self.total_acked < self.N) and (self.packet_idx < self.num_packets):
                    pkt = self.packets[self.packet_idx]
                    self.channel.log(f"[SEND] GBN Sender sent Packet (seq={pkt.seq_num}, payload='{pkt.payload}')")
                    if self.total_acked == self.packet_idx:
                        self.start_timer()
                    self.packet_idx += 1
                    self.nextseqnum = self.packet_idx % self.mod
                    self.channel.send_to_receiver(pkt)
            time.sleep(0.01)

        self.stop_timer()
        self.running = False
        ack_thread.join()

    def _listen_acks(self):
        while self.running or (self.total_acked < self.num_packets):
            try:
                ack_packet = self.channel.to_sender_queue.get(timeout=0.1)
                if not ack_packet.verify_checksum():
                    self.channel.log(f"[CORRUPT DISCARD] GBN Sender discarded corrupted ACK")
                    continue

                self.channel.log(f"[ACK RECEIVE] GBN Sender received Cumulative ACK (ack={ack_packet.ack_num})")
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
            except Empty:
                continue


class GBNReceiver:
    def __init__(self, num_packets, k, channel):
        self.num_packets = num_packets
        self.k = k
        self.mod = 2 ** k
        self.channel = channel
        self.expectedseqnum = 0
        self.delivered_count = 0
        self.finished = False

    def run(self):
        while not self.finished:
            try:
                pkt = self.channel.to_receiver_queue.get(timeout=0.1)
                if not pkt.verify_checksum():
                    self.channel.log(f"[CORRUPT DISCARD] GBN Receiver discarded corrupted Packet (seq={pkt.seq_num})")
                    last_ack = (self.expectedseqnum - 1) % self.mod
                    self.channel.send_to_sender(Packet(ack_num=last_ack, is_ack=True))
                    continue

                self.channel.log(f"[RECEIVE] GBN Receiver got Packet (seq={pkt.seq_num}, payload='{pkt.payload}')")
                if pkt.seq_num == self.expectedseqnum:
                    self.channel.log(f"[DELIVER] GBN Receiver delivered Packet (seq={pkt.seq_num}) to application")
                    ack = Packet(ack_num=self.expectedseqnum, is_ack=True)
                    self.channel.log(f"[ACK SEND] GBN Receiver sending Cumulative ACK (ack={self.expectedseqnum})")
                    self.expectedseqnum = (self.expectedseqnum + 1) % self.mod
                    self.delivered_count += 1
                    self.channel.send_to_sender(ack)
                else:
                    self.channel.log(f"[DISCARD] GBN Receiver discarded out-of-order Packet (seq={pkt.seq_num}, expected={self.expectedseqnum})")
                    last_ack = (self.expectedseqnum - 1) % self.mod
                    self.channel.log(f"[ACK SEND] GBN Receiver re-sending duplicate ACK (ack={last_ack})")
                    self.channel.send_to_sender(Packet(ack_num=last_ack, is_ack=True))
            except Empty:
                continue



print("GBN Implementation Faran Mukhtar - 24k-0646")
num_packets= int(input("Enter number of packets: "))
N= int(input ("Enter Window size(N): "))
k= int(input ("Enter Sequence number bits(K): "))
loss_prob=0
corrupt_prob=0
delay_prob=0
delay_sec=0
timeout_sec=0.8
print("=" * 80)
print(f"RUNNING GBN SIMULATION | Packets: {num_packets} | Window N: {N} | k-bits: {k} (mod {2**k})")
print(f"Channel Errors -> Loss: {loss_prob} | Corrupt: {corrupt_prob} | Delay: {delay_prob} ({delay_sec}s) | Timeout: {timeout_sec}s")
print("=" * 80)

random.seed(ROLL)
channel = UnreliableChannel(loss_prob, corrupt_prob, delay_prob, delay_sec)

sender = GBNSender(num_packets, N, k, timeout_sec, channel)
receiver = GBNReceiver(num_packets, k, channel)

rec_thread = threading.Thread(target=receiver.run)
snd_thread = threading.Thread(target=sender.run)

rec_thread.start()
snd_thread.start()

snd_thread.join()
receiver.finished = True
rec_thread.join()

print("=" * 80)
print(f"SUCCESS: All {num_packets} packets transmitted and delivered!")
elapsed = time.perf_counter() - start
print(f"Time elapsed: {elapsed}")

print("=" * 80 + "\n")