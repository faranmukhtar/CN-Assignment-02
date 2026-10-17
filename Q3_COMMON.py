import random
import socket
import threading
import time

ROLL_NUMBER_LAST_4 = 646
HOST = "127.0.0.1"
SENDER_ADDR = (HOST, 9001)
RECEIVER_ADDR = (HOST, 9002)
CHANNEL_ADDR = (HOST, 9000)

print_lock = threading.Lock()

def log(message):
  with print_lock:
    print(f'[{time.strftime("%H:%M:%S")}] {message}')

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

  def to_bytes(self):
    text = f'{int(self.is_ack)}|{self.seq_num}|{self.ack_num}|{self.payload}|{self.checksum}'
    return text.encode('utf-8')

  @staticmethod
  def from_bytes(data):
    
    is_ack, seq_num, ack_num, payload, checksum = data.decode('utf-8').split('|')
    pkt = Packet(int(seq_num), int(ack_num), payload, bool(int(is_ack)))
    pkt.checksum = int(checksum)
    return pkt

class UnreliableChannel:
  def __init__(self, loss_prob, corrupt_prob, delay_prob, delay_sec):
    self.loss_prob = loss_prob
    self.corrupt_prob = corrupt_prob
    self.delay_prob = delay_prob
    self.delay_sec = delay_sec
    self.running = True
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.sock.bind(CHANNEL_ADDR)
    self.sock.settimeout(0.1)

  def run(self):
    while self.running:
      try:
        data, addr = self.sock.recvfrom(2048)
      except (socket.timeout, ConnectionResetError):
        continue
      packet = Packet.from_bytes(data)
      if addr == SENDER_ADDR:
        dest = RECEIVER_ADDR
      else:
        dest = SENDER_ADDR
      self._process(packet, dest)

  def _process(self, packet, dest):
    if packet.is_ack:
      name = f'ACK (ack={packet.ack_num})'
    else:
      name = f'Data Packet (seq={packet.seq_num})'
    if random.random() < self.loss_prob:
      self.log_event('LOSS', f'Channel dropped {name}')
      return
    if random.random() < self.delay_prob:
      self.log_event('DELAY', f'Channel delaying {name} for {self.delay_sec}s')
      time.sleep(self.delay_sec)
    if random.random() < self.corrupt_prob:
      self.log_event('CORRUPT', f'Channel corrupted {name}')
      if packet.is_ack:
        packet.checksum = (packet.checksum + 1) % 65536
      else:
        packet.payload += '_CORRUPTED'
    self.sock.sendto(packet.to_bytes(), dest)

  def log_event(self, kind, message):
    log(f'[{kind}] {message}')