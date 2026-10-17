Transport Layer ARQ Protocols SimulatorOverview

Q3_COMMON.py: 
    Handles packet creation, checksum validation, and network channel simulation (loss, corruption, delay).

Q3_GBN.py:
    Implements Go-Back-N protocol using cumulative ACKs, a single sender timer, and fixed receiver expected sequence numbers.
 
Q3_SR.py:
    Implements Selective Repeat protocol using individual ACKs, per-packet sender timers, and receiver out-of-order buffering.
    
Parameters
    num_packets: Total packets to send.
    N: Window size.
    k: Sequence bits ($2^k$ sequence space).
    
    
How to RunBashpython 
Q3_GBN.py
python Q3_SR.py


Sample CommandBash$ python Q3_SR.py
Enter number of packets: 10
Enter Window size(N): 4
Enter Sequence number bits(K): 3