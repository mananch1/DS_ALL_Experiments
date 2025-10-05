import socket
import os
from time import time,ctime,sleep
import threading
from CAF import CAF_Clock
from Mutex import RicAgra

CAF = CAF_Clock(4002)

def call_rpc_fetch(a, host='localhost', port=12345):
    """Call the RPC server to add two numbers."""
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))

    # Send a request to the server to add numbers

    request = f"FETCH#{a}"
    client_socket.sendall(request.encode())

    # Wait for the response from the server
    response = client_socket.recv(1024)
    decoded = response.decode()
    print(f"Response from server: {decoded}")

    # Close the connection
    client_socket.close()







    


if __name__ == '__main__':
    
    RA = RicAgra(3002)
    while(1==1):
        sleep(2)
        print("Please enter Patient Id")
        Pid = input()
        RA.enter_CS()
        call_rpc_fetch(Pid)
        
        RA.exit_CS()
