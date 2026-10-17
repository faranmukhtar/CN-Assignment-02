import copy
import random
import threading
import time
from queue import Empty, Queue

# MY ROLE NUMBER IS 24k-0646 so seed is set to 646
ROLL_NUMBER_LAST_4 = 646

class Packet:
  def __init__(self, seq_num=0, ack_num=0, payload='', is_ack=False):
    self.seq_num = seq_num
    self.ack_num = ack_num
    self.payload = payload
    self.is_ack = is_ack
    self.checksum = self.compute_checksum()

  def compute_checksum(self):
    payload_sum = sum(self.payload.encode('utf-8'))
    total = self.seq_num + self.ack_num + payload_sum + int(self.is_ack)
    return total % 65536

  def verify_checksum(self):
    return self.checksum == self.compute_checksum()

class UnreliableChannel:
  def __init__(self, loss_prob, corrupt_prob, delay_prob, delay_sec):
    self.loss_prob = loss_prob
    self.corrupt_prob = corrupt_prob
    self.delay_prob = delay_prob
    self.delay_sec = delay_sec
    self.to_receiver_queue = Queue()
    self.to_sender_queue = Queue()
    self.print_lock = threading.Lock()

  def log(self, message):
    with self.print_lock:
      print(f'[{time.strftime("%H:%M:%S")}] {message}')

  def send_to_receiver(self, packet):
    pkt_copy = copy.deepcopy(packet)
    threading.Thread(
        target=self._process_to_receiver, args=(pkt_copy,), daemon=True
    ).start()

  def _process_to_receiver(self, packet):
    if random.random() < self.loss_prob:
      self.log(f'[LOSS] Channel dropped Data Packet (seq={packet.seq_num})')
      return
    if random.random() < self.delay_prob:
      self.log(
          f'[DELAY] Channel delaying Data Packet (seq={packet.seq_num}) for'
          f' {self.delay_sec}s'
      )
      time.sleep(self.delay_sec)
    if random.random() < self.corrupt_prob:
      self.log(f'[CORRUPT] Channel corrupted Data Packet (seq={packet.seq_num})')
      packet.payload += '_CORRUPTED'
    self.to_receiver_queue.put(packet)

  def send_to_sender(self, ack_packet):
    ack_copy = copy.deepcopy(ack_packet)
    threading.Thread(
        target=self._process_to_sender, args=(ack_copy,), daemon=True
    ).start()

  def _process_to_sender(self, ack_packet):
    if random.random() < self.loss_prob:
      self.log(f'[LOSS] Channel dropped ACK (ack={ack_packet.ack_num})')
      return
    if random.random() < self.delay_prob:
      self.log(
          f'[DELAY] Channel delaying ACK (ack={ack_packet.ack_num}) for'
          f' {self.delay_sec}s'
      )
      time.sleep(self.delay_sec)
    if random.random() < self.corrupt_prob:
      self.log(f'[CORRUPT] Channel corrupted ACK (ack={ack_packet.ack_num})')
      ack_packet.checksum = (ack_packet.checksum + 1) % 65536
    self.to_sender_queue.put(ack_packet)