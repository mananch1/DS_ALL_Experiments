import sys
import sqlite3
import threading
import uuid
import time
import xmlrpc.client
from xmlrpc.server import SimpleXMLRPCServer
from concurrent.futures import ThreadPoolExecutor
from CAF import CAF_Clock 
from Mutex import RicAgra

# --- Configuration ---
DATA_NODE_PEERS = [
    ('127.0.0.1', 7001),
    ('127.0.0.1', 7002),
    ('127.0.0.1', 7003),
]
QUORUM_W = 2
QUORUM_R = 2

# --- Global State ---
NODE_PORT = 0
DB_NAME_GLOBAL = "" # To store the database name for the dispatcher
caf_clock = None
RA = None

# --- Database Initialization (No changes needed) ---
def init_db(db_name):
    """Initializes the SQLite database with the updated schema (no logical_clock)."""
    conn = sqlite3.connect(db_name, check_same_thread=False)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            uuid TEXT PRIMARY KEY, username TEXT UNIQUE NOT NULL, first_name TEXT NOT NULL,
            last_name TEXT NOT NULL, dob TEXT NOT NULL, password TEXT NOT NULL
        )''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS records (
            record_id TEXT PRIMARY KEY, patient_uuid TEXT NOT NULL, doctor_name TEXT,
            description TEXT, resources_used TEXT, prescription TEXT,
            timestamp REAL,
            FOREIGN KEY (patient_uuid) REFERENCES users (uuid)
        )''')
    conn.commit()
    conn.close()
    print(f"Database '{db_name}' initialized with the new schema.")

# --- XML-RPC Client for Node-to-Node Communication ---
def send_rpc_to_peer(node_address, rpc_message):
    """Sends an RPC message to another data node (a peer) using XML-RPC."""
    host, port = node_address
    action = rpc_message['action']
    data = rpc_message['data']
    
    print(f"[*] DataNode-{NODE_PORT}: Replicating action '{action}' to peer http://{host}:{port}")
    try:
        proxy = xmlrpc.client.ServerProxy(f"http://{host}:{port}/", allow_none=True)
        return proxy.dispatch_rpc(action, data)
    except (ConnectionRefusedError, xmlrpc.client.ProtocolError):
        return {"status": "error", "message": f"Peer {host}:{port} is offline."}
    except Exception as e:
        return {"status": "error", "message": f"RPC to peer error: {e}"}

# --- Internal Replication Handler (No changes needed) ---
def handle_replicate_write(cursor, data):
    """Handles a write request from a peer node, updated for the new schema."""
    record_type = data.get("record_type")
    print(f"[*] DataNode-{NODE_PORT}: Received replication request for type '{record_type}'")
    record_data = data.get("record_data")
    if record_type == "user":
        cursor.execute(
            "INSERT OR REPLACE INTO users (uuid, username, first_name, last_name, dob, password) VALUES (?, ?, ?, ?, ?, ?)",
            (record_data['uuid'], record_data['username'], record_data['first_name'], record_data['last_name'], record_data['dob'], record_data['password'])
        )
    elif record_type == "record":
        cursor.execute(
            "INSERT OR REPLACE INTO records (record_id, patient_uuid, doctor_name, description, resources_used, prescription, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (record_data['record_id'], record_data['patient_uuid'], record_data['doctor_name'], record_data['description'], record_data.get('resources_used'), record_data['prescription'], record_data['timestamp'])
        )
    else:
        return {"status": "error", "message": "Unknown record type for replication"}
    return {"status": "success", "message": "Replication successful"}

# --- Quorum Write Helper (No changes needed) ---
def perform_quorum_write(cursor, record_type, record_data):
    handle_replicate_write(cursor, {"record_type": record_type, "record_data": record_data})
    
    replication_payload = {
        "action": "replicate_write",
        "data": {"record_type": record_type, "record_data": record_data}
    }
    peers = [p for p in DATA_NODE_PEERS if p[1] != NODE_PORT]
    
    ack_count = 1
    ack_lock = threading.Lock()
    
    def replicate_and_count(peer):
        nonlocal ack_count
        response = send_rpc_to_peer(peer, replication_payload)
        if response and response.get("status") == "success":
            with ack_lock:
                ack_count += 1

    with ThreadPoolExecutor(max_workers=len(peers)) as executor:
        executor.map(replicate_and_count, peers)

    return ack_count >= QUORUM_W, ack_count

# --- External Action Handlers (No changes needed) ---
def handle_add_account(cursor, data):
    print(f"[*] DataNode-{NODE_PORT}: Handling 'add_account' for user '{data.get('username')}'")
    user_uuid = str(uuid.uuid4())
    user_data = {
        'uuid': user_uuid, 'username': data['username'], 'first_name': data['first_name'],
        'last_name': data['last_name'], 'dob': data['dob'], 'password': data['password']
    }
    success, acks = perform_quorum_write(cursor, "user", user_data)
    if success:
        print(f"[SUCCESS] DataNode-{NODE_PORT}: Account for '{data.get('username')}' replicated with {acks} acks.")
        return {"status": "success", "code": 201, "message": "Account created", "uuid": user_uuid}
    else:
        print(f"[FAILURE] DataNode-{NODE_PORT}: Quorum failed for 'add_account' ({acks}/{QUORUM_W})")
        return {"status": "error", "code": 500, "error": f"Quorum failed. Only {acks}/{QUORUM_W} nodes acknowledged."}

def handle_add_record(cursor, data):
    global caf_clock
    print(f"[*] DataNode-{NODE_PORT}: Handling 'add_record' for patient UUID '{data.get('patient_uuid')}'")
    record_id = str(uuid.uuid4())
    synchronized_time = (time.time() + caf_clock.CAF) * 1000
    
    new_record = {
        "record_id": record_id, "patient_uuid": data['patient_uuid'], "doctor_name": data['doctor_name'], 
        "description": data['description'], "resources_used": data.get('resources_used'), "prescription": data['prescription'],
        "timestamp": synchronized_time 
    }
    
    success, acks = perform_quorum_write(cursor, "record", new_record)
    if success:
        print(f"[SUCCESS] DataNode-{NODE_PORT}: Record for patient '{data.get('patient_uuid')}' replicated with {acks} acks.")
        return {"status": "success", "code": 201, "message": f"Record added and replicated to {acks} nodes.", "record_id": record_id}
    else:
        print(f"[FAILURE] DataNode-{NODE_PORT}: Quorum failed for 'add_record' ({acks}/{QUORUM_W})")
        return {"status": "error", "code": 500, "error": f"Quorum failed. Only {acks}/{QUORUM_W} nodes acknowledged."}

def handle_get_data(cursor, data):
    global RA
    print(f"[*] DataNode-{NODE_PORT}: Handling 'get_data' for user '{data.get('username')}'")
    RA.enter_CS(time=time.time() + caf_clock.CAF)
    cursor.execute("SELECT * FROM users WHERE username = ?", (data.get('username'),))
    user_row = cursor.fetchone()
    RA.exit_CS()
    if user_row is None:
        return {"status": "error", "code": 404, "error": "User not found"}
    user_columns = [desc[0] for desc in cursor.description]
    user_data = dict(zip(user_columns, user_row))
    if user_data.get('password') != data.get('password'):
        return {"status": "error", "code": 401, "error": "Authentication failed"}
    del user_data['password']
    
    RA.enter_CS(time=time.time() + caf_clock.CAF)
    cursor.execute("SELECT * FROM records WHERE patient_uuid = ? ORDER BY timestamp DESC", (user_data['uuid'],))
    record_rows = cursor.fetchall()
    RA.exit_CS()
    record_columns = [desc[0] for desc in cursor.description]
    records_list = [dict(zip(record_columns, row)) for row in record_rows]
    return {"status": "success", "code": 200, "data": {"user_info": user_data, "records": records_list}}

def handle_get_records_by_uuid(cursor, data):
    print(f"[*] DataNode-{NODE_PORT}: Handling 'get_records_by_uuid' for patient UUID '{data.get('uuid')}'")
    patient_uuid = data.get('uuid')
    RA.enter_CS(time=time.time() + caf_clock.CAF)
    cursor.execute("SELECT * FROM records WHERE patient_uuid = ? ORDER BY timestamp DESC", (patient_uuid,))
    record_rows = cursor.fetchall()
    RA.exit_CS()
    if not record_rows:
        return {"status": "success", "code": 200, "data": []}
    record_columns = [desc[0] for desc in cursor.description]
    records_list = [dict(zip(record_columns, row)) for row in record_rows]
    return {"status": "success", "code": 200, "data": records_list}

def get_all_patients_legacy(cursor, data):
    global caf_clock
    print(f"[*] DataNode-{NODE_PORT}: Handling LEGACY 'get_all_patients'")
    RA.enter_CS(time=time.time() + caf_clock.CAF)
    cursor.execute("SELECT uuid, first_name, last_name, dob FROM users")
    rows = cursor.fetchall()
    RA.exit_CS()
    patients = {row[0]: {"patient_id": row[0], "name": f"{row[1]} {row[2]}", "dob": row[3]} for row in rows}
    return {"status": "success", "data": patients}

# Mapping actions to functions
ACTION_MAP = {
    "add_account": handle_add_account, "add_record": handle_add_record,
    "get_data": handle_get_data, "get_records_by_uuid": handle_get_records_by_uuid,
    "replicate_write": handle_replicate_write, "get_all_patients": get_all_patients_legacy,
}

# --- XML-RPC Dispatcher ---
def dispatch_rpc(action, data):
    """Main dispatcher for all incoming XML-RPC calls."""
    print(f"\n--- DataNode-{NODE_PORT}: Received XML-RPC call for action: '{action}' ---")
    
    response = {"status": "error", "code": 400, "message": "Unknown action"}
    if action not in ACTION_MAP:
        return response

    conn = sqlite3.connect(DB_NAME_GLOBAL, check_same_thread=False)
    cursor = conn.cursor()
    try:
        response = ACTION_MAP[action](cursor, data)
        conn.commit()
    except sqlite3.IntegrityError:
        conn.rollback()
        response = {"status": "error", "code": 409, "error": "Username already exists or constraint failed"}
    except Exception as e:
        conn.rollback()
        response = {"status": "error", "code": 500, "message": f"An internal error occurred: {e}"}
        print(f"[ERROR] Exception during action '{action}': {e}")
    finally:
        conn.close()
        
    return response

# --- Main Server Function ---
def main(port, db_name):
    global NODE_PORT, caf_clock, RA, DB_NAME_GLOBAL
    NODE_PORT = port
    DB_NAME_GLOBAL = db_name
    
    # Initialize CAF and Ricart-Agrawala services
    caf_port = port - 3000
    RA_port = port - 4000
    caf_clock = CAF_Clock(caf_port)
    RA = RicAgra(RA_port)
    print(f"CAF Clock synchronization service started on port {caf_port}")
    
    init_db(db_name)
    host = '127.0.0.1'
    
    # Setup and run the XML-RPC server
    with SimpleXMLRPCServer((host, port), allow_none=True) as server:
        server.register_introspection_functions()
        
        # Register the single dispatcher function to handle all requests
        server.register_function(dispatch_rpc, 'dispatch_rpc')
        
        print(f"Data Node XML-RPC server is listening on {host}:{port}, using database '{db_name}'")
        server.serve_forever()

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: python data_node.py <port> <db_name>")
        sys.exit(1)
    port_arg = int(sys.argv[1])
    db_name_arg = sys.argv[2]
    main(port_arg, db_name_arg)