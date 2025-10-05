import socket
import sqlite3
import os
from time import time,ctime
from Mutex import RicAgra
from CAF import CAF_Clock
CAF = CAF_Clock(4003)

DB_NAME='db-server.db'
def insert_person(name, age):
    """
    Inserts a person's name and age into the 'people' table.

    Args:
        name (str): The name of the person.
        age (int): The age of the person.

    Returns:
        bool: True if insertion was successful, False otherwise.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO patient (name, age) VALUES (?, ?)", (name, age))
        conn.commit()
        return(f"Successfully added '{name}' (Age: {age}) to the database.")

    except sqlite3.Error as e:
        return(f"Error adding person to database: {e}")

    finally:
        if conn:
            conn.close()

def fetch_person(pid):
    """
    Inserts a person's name and age into the 'people' table.

    Args:
        name (str): The name of the person.
        age (int): The age of the person.

    Returns:
        bool: True if insertion was successful, False otherwise.
    """
    conn = None
    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM patient WHERE pid='{pid}'")
        res = cursor.fetchall()
        conn.commit()
        return(res)

    except sqlite3.Error as e:
        return(f"Error adding person to database: {e}")

    finally:
        if conn:
            conn.close()

def add_entry(a, b):
    """A simple function to add two numbers."""
    return a + b

def start_rpc_server(host='0.0.0.0', port=12345):
    """Starts the server that listens for incoming RPC requests."""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(1)
    print(f"Server listening on {host}:{port}...")

    while True:
        # Accept client connection
        conn, addr = server_socket.accept()
        print(f"Connected to {addr}")


        data = conn.recv(1024)  # 1024 bytes at a time
        if data:
            # Decode the data into a string and process it
            request = data.decode()

            if request.startswith("ADD"):
                # Extract numbers from the request
                _, a, b = request.split('#')
                result = insert_person(a,int(b))
                response = f"Result: {result}"

                # Send the result back to the client
                conn.sendall(response.encode())
            if request.startswith("FETCH"):
                # Extract numbers from the request
                _, a= request.split('#')
                result = fetch_person(a)
                response = f"Result: {result}"

                # Send the result back to the client
                conn.sendall(response.encode())
        conn.close()



    

if __name__ == '__main__':
    RA = RicAgra(port=3003)
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS patient (
                    pid INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    age INTEGER NOT NULL
                    );""")
    
    conn.commit()
    conn.close()
    
    start_rpc_server()
