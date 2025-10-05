import threading
import socket
from time import sleep, time
from sys import maxsize

class RicAgra:
    # --- STATE VARIABLES ---
    IN_FLAG = False      # True if we are in or want to enter the CS
    ABORTED = False      # True if we received a "DIE" message
    IN_TIME = maxsize    # Timestamp of our request to enter the CS
    IN_QUEUE = []        # Queue for deferred requests (from older processes)
    OK_COUNT = 0         # Counter for received OK messages
    port = 0
    BIRTH_TIME = 0       # Unique timestamp for process age

    def __init__(self, port):
        """Initializes the process and starts its listening daemon."""
        self.port = port
        self.BIRTH_TIME = time()  # Set birth time at initialization
        ra_deamon = threading.Thread(target=self.ric_agra_deamon, args=[port,], daemon=True)
        ra_deamon.start()

    # --- SENDER METHODS ---
    def _send_message(self, port, message):
        """A helper function to send a message to a specific port."""
        try:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect(('localhost', port))
            client_socket.sendall(message.encode())
            client_socket.close()
        except ConnectionRefusedError:
            print(f"[{self.port}] Connection to {port} refused.")

    def send_ok(self, addr):
        """Sends an OK message."""
        print(f"[{self.port}] Sending OK to {addr[1]}")
        self._send_message(addr[1], 'OK')

    def send_die(self, addr):
        """Sends a DIE message."""
        print(f"[{self.port}] Sending DIE to {addr[1]}")
        self._send_message(addr[1], 'DIE')

    # --- LISTENING DAEMON ---
    def ric_agra_deamon(self, port=3000):
        """Starts the server that listens for incoming requests from other processes."""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('0.0.0.0', port))
        server_socket.listen(5)
        while True:
            conn, _ = server_socket.accept()
            data = conn.recv(1024)
            conn.close()

            if not data:
                continue
            
            request = data.decode()

            if request.startswith("REQ"):
                parts = request.split("#")
                incoming_time = float(parts[1])
                incoming_port = int(parts[2])
                incoming_birth = float(parts[3])
                addr = ['localhost', incoming_port]

                if self.IN_FLAG:
                    # Wait-Die Algorithm: older process waits, younger dies
                    if incoming_birth < self.BIRTH_TIME:
                        print(f"[{self.port}] Deferring request from older process {incoming_port}")
                        self.IN_QUEUE.append(addr)
                    else:
                        print(f"[{self.port}] Denying request from younger process {incoming_port}")
                        self.send_die(addr)
                else:
                    self.send_ok(addr)

            elif request.startswith("OK"):
                self.OK_COUNT += 1
                print(f"[{self.port}] OK received. Count: {self.OK_COUNT}")

            elif request.startswith("DIE"):
                print(f"[{self.port}] DIE received. Aborting request.")
                self.ABORTED = True

    # --- MUTUAL EXCLUSION API ---
    def enter_CS(self, ip_list=[3001, 3002, 3003]):
        """Requests entry to the Critical Section using Wait-Die algorithm."""
        peers = [p for p in ip_list if p != self.port]
        
        while True:
            self.IN_TIME = time()
            self.IN_FLAG = True
            self.ABORTED = False
            self.OK_COUNT = 0
            self.IN_QUEUE = []

            print(f"[{self.port}] Requesting CS with timestamp: {self.IN_TIME}")
            request_msg = f"REQ#{self.IN_TIME}#{self.port}#{self.BIRTH_TIME}"
            for p_port in peers:
                self._send_message(p_port, request_msg)

            while self.OK_COUNT < len(peers):
                if self.ABORTED:
                    print(f"[{self.port}] Request aborted. Retrying after a short delay...")
                    sleep(10)
                    break
                sleep(0.1)

            if self.ABORTED:
                continue
            else:
                print(f"[{self.port}] All OKs received. Entering Critical Section.")
                break

    def exit_CS(self):
        """Exits the Critical Section and grants permission to waiting processes."""
        print(f"[{self.port}] Exiting Critical Section.")
        self.IN_FLAG = False
        for addr in self.IN_QUEUE:
            self.send_ok(addr)
        self.IN_QUEUE = []
