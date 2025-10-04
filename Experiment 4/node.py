import socket
import sys
import random
from time import time,ctime,sleep
import threading


def cv_get(host='localhost', port=4000):
    """Call the RPC server to add two numbers."""

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))
    
	
    request = "GET TIME "+str(time())
    client_socket.sendall(request.encode())
    
    # Wait for the response from the server
    response = client_socket.recv(1024)
    print(f"Response from server: {response.decode()}")

    client_socket.close()
    return float(response.decode())


def caf_send(caf,host='localhost', port=4000):
    """Call the RPC server to add two numbers."""

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((host, port))
    
	
    request = f"CAF {caf}"
    client_socket.sendall(request.encode())
    
    # Wait for the response from the server
    response = client_socket.recv(1024)
    print(f"Response from server: {response.decode()}")

    client_socket.close()


def time_get_daemon():
    while True:
        sleep(random.randint(1,10))
        slave_list=[4001,4002,4003]
        cv_list=[]
        for slave in slave_list:
            cv_list.append(cv_get(port=slave))
        
        avg_cv = sum(cv_list)/len(cv_list)
        print("cv avg",avg_cv)

        
        caf = avg_cv - cv_list[0]
        print('NEW CAF IS',caf)
        sleep(random.randint(600,1200))
    
def time_send_daemon(port=4000):
    """Starts the server that listens for incoming RPC requests."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', port))
    server_socket.listen(1)
    print(f"Server listening on {'0.0.0.0'}:{port}...")

    while True:
        # Accept client connection
        conn, addr = server_socket.accept()
        


        data = conn.recv(1024)  # 1024 bytes at a time
        if data:
            # Decode the data into a string and process it
            request = data.decode()
            if(request.startswith("GET TIME")):
                a = request.split(' ')
                cv=str(time()-float(a[2]))
                conn.sendall(cv.encode())
        conn.close()

if(__name__=='__main__'):
    if len(sys.argv) != 2:
        print("Usage: python app_node.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    send_deamon=threading.Thread(target=time_send_daemon,daemon=True,args=(port,))
    get_deamon=threading.Thread(target=time_get_daemon,daemon=True)
    send_deamon.start()
    
    get_deamon.start()

    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        print("Shutting down.")