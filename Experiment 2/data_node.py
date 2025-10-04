import sys
import socket
import sqlite3
import json
import threading
import uuid
import time
from concurrent.futures import ThreadPoolExecutor

# --- Database Initialization ---
def init_db(db_name):
    """Initializes the SQLite database with the new, merged schema."""
    conn = sqlite3.connect(db_name, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            uuid TEXT PRIMARY KEY, username TEXT UNIQUE NOT NULL, first_name TEXT NOT NULL,
            last_name TEXT NOT NULL, dob TEXT NOT NULL, password TEXT NOT NULL
        )''')
    conn.commit()
    conn.close()
    print(f"Database '{db_name}' initialized with the new schema.")

# --- External Action Handlers ---
def handle_add_account(cursor, data):
    print(f"[*] DataNode-{NODE_PORT}: Handling 'add_account' for user '{data.get('username')}'")
    user_uuid = str(uuid.uuid4())
    user_data = {
        'uuid': user_uuid, 'username': data['username'], 'first_name': data['first_name'],
        'last_name': data['last_name'], 'dob': data['dob'], 'password': data['password']
    }

    cursor.execute(
            "INSERT OR REPLACE INTO users (uuid, username, first_name, last_name, dob, password) VALUES (?, ?, ?, ?, ?, ?)",
            (user_data['uuid'], user_data['username'], user_data['first_name'], user_data['last_name'], user_data['dob'], user_data['password'])
        )
    
    print(f"[SUCCESS] DataNode-{NODE_PORT}: Account for '{data.get('username')}'")
    return {"status": "success", "code": 201, "message": "Account created", "uuid": user_uuid}
    

def handle_get_data(cursor, data):
    print(f"[*] DataNode-{NODE_PORT}: Handling 'get_data' for user '{data.get('username')}'")
    cursor.execute("SELECT * FROM users WHERE username = ?", (data.get('username'),))
    user_row = cursor.fetchone()
    if user_row is None:
        return {"status": "error", "code": 404, "error": "User not found"}
    user_columns = [desc[0] for desc in cursor.description]
    user_data = dict(zip(user_columns, user_row))
    if user_data.get('password') != data.get('password'):
        return {"status": "error", "code": 401, "error": "Authentication failed"}
    del user_data['password']
    return {"status": "success", "code": 200, "data": {"user_info": user_data}}


ACTION_MAP = {
    "add_account": handle_add_account,
    "get_data": handle_get_data,
}

# --- Client Handler Thread ---
def handle_client(connection, db_name, addr):
    try:
        data = connection.recv(4096).decode('utf-8')
        if not data: return
        
        print(f"\n--- DataNode-{NODE_PORT}: Received connection from {addr} ---")
        message = json.loads(data)
        action = message.get("action")
        print(f"[*] DataNode-{NODE_PORT}: Action requested: '{action}'")
        
        
        response = {"status": "error", "message": "Unknown action"}
        if action in ACTION_MAP:
            conn = sqlite3.connect(db_name, check_same_thread=False)
            cursor = conn.cursor()
            try:
                response = ACTION_MAP[action](cursor, message.get("data"))
                conn.commit()
            except sqlite3.IntegrityError:
                conn.rollback()
                response = {"status": "error", "code": 409, "error": "Username already exists"}
            except Exception as e:
                conn.rollback()
                response = {"status": "error", "code": 500, "message": f"Database error: {e}"}
            finally:
                conn.close()
        
        connection.sendall(json.dumps(response).encode('utf-8'))
    except Exception as e:
        print(f"An error occurred while handling client {addr}: {e}")
    finally:
        connection.close()

# --- Main Server Function ---
def main(port, db_name):
    global NODE_PORT
    NODE_PORT = port
    init_db(db_name)
    host = '127.0.0.1'
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, port))
        s.listen()
        print(f"Data Node is listening on {host}:{port}, using database '{db_name}'")
        while True:
            conn, addr = s.accept()
            thread = threading.Thread(target=handle_client, args=(conn, db_name, addr))
            thread.start()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python data_node.py <port> <db_name>")
        sys.exit(1)
    port_arg = int(sys.argv[1])
    db_name_arg = sys.argv[2]
    main(port_arg, db_name_arg)
