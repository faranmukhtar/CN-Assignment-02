import random
import time
import threading
from queue import Queue, Empty
from Q3_COMMON import Packet , UnreliableChannel

# MY ROLE NUMBER IS 24k-0646 so seed is set to 646
ROLL = 646

class SRSender:
    def __init__(self, num_packets, N, k, timeout_sec, channel):
        self.num_packets = num_packets
        self.N = N
        self.k = k
        self.mod = 2 ** k
        self.timeout_sec = timeout_sec
        self.channel = channel
        self.send_base = 0
        self.nextseqnum = 0
        self.packets = [Packet(seq_num=i % self.mod, payload=f"Data_{i}") for i in range(num_packets)]
        self.acked = [False] * num_packets
        self.timers = {}
        self.lock = threading.Lock()
        self.running = False

    def start_packet_timer(self, pkt_idx):
        self.stop_packet_timer(pkt_idx)
        timer = threading.Timer(self.timeout_sec, self.on_packet_timeout, args=(pkt_idx,))
        self.timers[pkt_idx] = timer
        timer.start()

    def stop_packet_timer(self, pkt_idx):
        if pkt_idx in self.timers and self.timers[pkt_idx]:
            self.timers[pkt_idx].cancel()
            del self.timers[pkt_idx]

    def on_packet_timeout(self, pkt_idx):
        with self.lock:
            if not self.acked[pkt_idx]:
                pkt = self.packets[pkt_idx]
                self.channel.log(f"[TIMEOUT] SR Timer expired for Packet (idx={pkt_idx}, seq={pkt.seq_num})")
               
                self.start_packet_timer(pkt_idx)
                self.channel.log(f"[RETRANSMIT] SR Resending missing Packet (seq={pkt.seq_num}, payload='{pkt.payload}')")
                self.channel.send_to_receiver(pkt)

    def run(self):
        self.running = True
        
        ack_thread = threading.Thread(target=self._listen_acks, daemon=True)
        ack_thread.start()

        while self.send_base < self.num_packets:
            with self.lock:
                if (self.nextseqnum - self.send_base < self.N) and (self.nextseqnum < self.num_packets):
                    pkt = self.packets[self.nextseqnum]
                    self.channel.log(f"[SEND] SR Sender sent Packet (seq={pkt.seq_num}, payload='{pkt.payload}')")
                    self.start_packet_timer(self.nextseqnum)
                    self.channel.send_to_receiver(pkt)
                    self.nextseqnum += 1
            time.sleep(0.01)

        self.running = False
        ack_thread.join()

    def _listen_acks(self):
        while self.running or (self.send_base < self.num_packets):
            try:
                ack_packet = self.channel.to_sender_queue.get(timeout=0.1)
                if not ack_packet.verify_checksum():
                    self.channel.log(f"[CORRUPT DISCARD] SR Sender discarded corrupted ACK")
                    continue

                self.channel.log(f"[ACK RECEIVE] SR Sender received Individual ACK (ack={ack_packet.ack_num})")
                with self.lock:
                    for idx in range(self.send_base, min(self.send_base + self.N, self.num_packets)):
                        if self.packets[idx].seq_num == ack_packet.ack_num and not self.acked[idx]:
                            self.acked[idx] = True
                            self.stop_packet_timer(idx)
                            self.channel.log(f"[ACKED] SR Sender marked Packet idx={idx} (seq={ack_packet.ack_num}) as ACKed")
                            break

                    while self.send_base < self.num_packets and self.acked[self.send_base]:
                        self.send_base += 1
            except Empty:
                continue


class SRReceiver:
    def __init__(self, num_packets, N, k, channel):
        self.num_packets = num_packets
        self.N = N
        self.k = k
        self.mod = 2 ** k
        self.channel = channel
        self.rcv_base = 0
        self.buffer = {}
        self.received = [False] * num_packets
        self.delivered_count = 0
        self.finished = False

    def run(self):
        while not self.finished:
            try:
                pkt = self.channel.to_receiver_queue.get(timeout=0.1)
                if not pkt.verify_checksum():
                    self.channel.log(f"[CORRUPT DISCARD] SR Receiver discarded corrupted Packet (seq={pkt.seq_num})")
                    continue

                self.channel.log(f"[RECEIVE] SR Receiver got Packet (seq={pkt.seq_num}, payload='{pkt.payload}')")

                found_idx = -1
                for idx in range(self.rcv_base, min(self.rcv_base + self.N, self.num_packets)):
                    if (idx % self.mod) == pkt.seq_num and not self.received[idx]:
                        found_idx = idx
                        break

                if found_idx != -1:
                    ack = Packet(ack_num=pkt.seq_num, is_ack=True)
                    self.channel.log(f"[ACK SEND] SR Receiver sending Individual ACK (ack={pkt.seq_num})")
                    self.channel.send_to_sender(ack)
                    self.received[found_idx] = True
                    self.buffer[found_idx] = pkt
                    self.channel.log(f"[BUFFER] SR Receiver buffered out-of-order Packet (idx={found_idx}, seq={pkt.seq_num})")

                    while self.rcv_base < self.num_packets and self.received[self.rcv_base]:
                        deliv_pkt = self.buffer[self.rcv_base]
                        self.channel.log(f"[DELIVER] SR Receiver delivered Packet (idx={self.rcv_base}, seq={deliv_pkt.seq_num}) in order")
                        self.delivered_count += 1
                        self.rcv_base += 1
                else:
                    prev_start = max(0, self.rcv_base - self.N)
                    for idx in range(prev_start, self.rcv_base):
                        if (idx % self.mod) == pkt.seq_num:
                            ack = Packet(ack_num=pkt.seq_num, is_ack=True)
                            self.channel.log(f"[ACK SEND] SR Receiver re-sending Individual ACK (ack={pkt.seq_num}) for duplicate")
                            self.channel.send_to_sender(ack)
                            break
            except Empty:
                continue


print("SR Implementation Faran Mukhtar - 24k-0646")
num_packets= int(input("Enter number of packets: "))
N= int(input ("Enter Window size(N): "))
k= int(input ("Enter Sequence number bits(K): "))
loss_prob=0.1
corrupt_prob=0.1
delay_prob=0.1
delay_sec=0.2
timeout_sec=0.8

print("=" * 80)
print(f"RUNNING GBN SIMULATION | Packets: {num_packets} | Window N: {N} | k-bits: {k} (mod {2**k})")
print(f"Channel Errors -> Loss: {loss_prob} | Corrupt: {corrupt_prob} | Delay: {delay_prob} ({delay_sec}s) | Timeout: {timeout_sec}s")
print("=" * 80)

random.seed(ROLL)
channel = UnreliableChannel(loss_prob, corrupt_prob, delay_prob, delay_sec)


sender = SRSender(num_packets, N, k, timeout_sec, channel)
receiver = SRReceiver(num_packets, N, k, channel)
rec_thread = threading.Thread(target=receiver.run)
snd_thread = threading.Thread(target=sender.run)
rec_thread.start()
snd_thread.start()

snd_thread.join()
receiver.finished = True
rec_thread.join()

print("=" * 80)
print(f"SUCCESS: All {num_packets} packets transmitted and delivered!")
print("=" * 80 + "\n")
