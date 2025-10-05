import socket
import os
from time import time,ctime,sleep
import threading
from CAF import CAF_Clock
from Mutex import RicAgra

CAF = CAF_Clock(4001)


def call_rpc_add(a, b, host='localhost', port=12345):
    
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))
   
    # Send a request to the server to add numbers

    request = f"ADD#{a}#{b}"
    client_socket.sendall(request.encode())
   
    # Wait for the response from the server
    response = client_socket.recv(1024)
    print(f"Response from server: {response.decode()}")
   
    # Close the connection
    client_socket.close()




   



if __name__ == '__main__':

    
    RA = RicAgra(3001)
   
    while(1==1):
        sleep(2)
        print("Please enter Name")
        name = input()
        RA.enter_CS(time=time()+CAF.CAF)
        print("Please enter Age")
        age = input()
        call_rpc_add(name, age)  # Example: Add 3 and 5
        IN_FLAG=False
        RA.exit_CS()
            
            
            