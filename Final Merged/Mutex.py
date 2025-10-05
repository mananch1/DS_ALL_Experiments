import threading
import socket
from time import sleep
from sys import maxsize

class RicAgra:
    IN_FLAG = False
    IN_TIME = maxsize
    IN_QUEUE = []
    OK_COUNT = 0
    port = 0

    def __init__(self,port):
        ra_deamon = threading.Thread(target=self.ric_agra_deamon,args=[port,],daemon=True)
        ra_deamon.start()
        self.port = port
        
    def send_ok(self,addr):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(('localhost',addr[1]))
        client_socket.sendall('OK'.encode())
        client_socket.close()


    def ric_agra_deamon(self,port=3000):
        """Starts the server that listens for incoming RPC requests."""
        
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(('0.0.0.0', port))
        server_socket.listen(1)
        while True:
            # Accept client connection
            conn,addr = server_socket.accept()
            
            addr = conn.getsockname()
            
            data = conn.recv(1024)  # 1024 bytes at a time
            conn.close()

        

            if data:
                # Decode the data into a string and process it
                request = data.decode()

                

                if request.startswith("REQ"):
                    a = request.split("#")
                    addr = ['localhost',int(a[2])]
                    
                    # Extract numbers from the request
                    if(self.IN_FLAG):
                        if(float(a[1])<self.IN_TIME):
                            self.send_ok(addr)
                        else:
                            self.IN_QUEUE.append(addr)
                            
                    else:
                        
                        self.send_ok(addr)

                    # Send the result back to the client

                if request.startswith("OK"):
                    self.OK_COUNT+=1

    def enter_CS(self,time,ip_list=[3001,3002,3003]):
        """Starts the server that listens for incoming RPC requests."""
        """Call the RPC server to add two numbers."""
        # Send a request to the server to add numbers
         
        self.IN_TIME=time
        self.IN_FLAG = True

        n = len(ip_list)

        for ip in ip_list:
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if(ip!=self.port):
                client_socket.connect(('localhost', ip))

                request = f"REQ#"+str(self.IN_TIME)+"#"+str(self.port)
                client_socket.sendall(request.encode())

                # Close the connection
                client_socket.close()

        while(self.OK_COUNT!=len(ip_list)-1):
            sleep(2)
        self.OK_COUNT=0
            
    
        

    def exit_CS(self):
        self.IN_FLAG=False
        for addr in self.IN_QUEUE:
            self.send_ok(addr)
        self.IN_QUEUE=[]
        