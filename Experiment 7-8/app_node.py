import sys
import xmlrpc.client
from itertools import cycle
from flask import Flask, request, jsonify
from flask_cors import CORS
import redis # Import the redis library
import json # Import the json library for serialization

# --- Configuration ---
DATA_NODES = [
    ('127.0.0.1', 7001),
    ('127.0.0.1', 7002),
    ('127.0.0.1', 7003),
]
data_node_cycler = cycle(DATA_NODES)

# --- REDIS CACHE Configuration ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
CACHE_TTL_SECONDS = 300 # Time-To-Live for cache entries: 5 minutes

# --- Initialize Flask App and Redis ---
app = Flask(__name__)
CORS(app)

# Establish connection to Redis
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_client.ping() # Check if the connection is successful
    print(f"[*] Successfully connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except redis.exceptions.ConnectionError as e:
    print(f"[ERROR] Could not connect to Redis: {e}")
    print("[WARNING] Caching will be disabled.")
    redis_client = None


# --- XML-RPC Client Function (No changes needed) ---
def send_rpc_to_data_node(rpc_message):
    """Sends a message to a Data Node using XML-RPC."""
    node_host, node_port = next(data_node_cycler)
    action = rpc_message['action']
    data = rpc_message['data']
    
    print(f"[*] AppNode-{app.port}: Forwarding action '{action}' to DataNode http://{node_host}:{node_port}")
    
    try:
        proxy = xmlrpc.client.ServerProxy(f"http://{node_host}:{node_port}/", allow_none=True)
        response_json = proxy.dispatch_rpc(action, data)
        print(f"[*] AppNode-{app.port}: Received response from DataNode: {response_json}")
        return response_json
        
    except (ConnectionRefusedError, xmlrpc.client.ProtocolError) as e:
        error_msg = f"Data service at {node_host}:{node_port} is unavailable."
        print(f"[ERROR] {error_msg} - {e}")
        return {"status": "error", "code": 503, "error": error_msg}
        
    except Exception as e:
        error_msg = f"An unexpected RPC error occurred: {e}"
        print(f"[ERROR] {error_msg}")
        return {"status": "error", "code": 500, "error": error_msg}

# --- API Endpoints (Updated with Caching Logic) ---

@app.route('/add_account', methods=['POST'])
def add_account():
    print(f"\n--- AppNode-{app.port}: Received request for /add_account ---")
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400
    
    payload = request.get_json()
    print(f"[*] AppNode-{app.port}: Payload: {payload}")
    rpc_payload = {"action": "add_account", "data": payload}
    rpc_response = send_rpc_to_data_node(rpc_payload)
    
    # WRITE-THROUGH: Invalidate cache after successful DB write
    if redis_client and rpc_response.get('status') == 'success':
        # Adding a new account invalidates the list of all patients
        print("[*] Cache Invalidation: Deleting 'all_patients' key.")
        redis_client.delete("all_patients")

    status_code = rpc_response.get('code', 500)
    return jsonify(rpc_response), status_code

@app.route('/get_data/<string:username>', methods=['GET'])
def get_data(username):
    print(f"\n--- AppNode-{app.port}: Received request for /get_data/{username} ---")
    auth = request.authorization
    if not auth or not auth.username or not auth.password:
        return jsonify({"error": "Authentication required"}), 401
    
    # CACHE LOGIC: Check cache first for this user's data
    cache_key = f"user_data:{auth.username}"
    if redis_client:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            print(f"[*] Cache Hit for key: '{cache_key}'")
            return jsonify(json.loads(cached_data)), 200

    print(f"[*] Cache Miss for key: '{cache_key}'. Fetching from DataNode.")
    print(f"[*] AppNode-{app.port}: Authenticating user '{auth.username}'")
    payload = {"username": auth.username, "password": auth.password}
    rpc_payload = {"action": "get_data", "data": payload}
    rpc_response = send_rpc_to_data_node(rpc_payload)
    
    # CACHE LOGIC: Populate cache on successful DB read
    if redis_client and rpc_response.get('status') == 'success':
        data_to_cache = rpc_response.get('data')
        if data_to_cache:
            print(f"[*] Populating cache for key: '{cache_key}' with TTL {CACHE_TTL_SECONDS}s.")
            redis_client.set(cache_key, json.dumps(data_to_cache), ex=CACHE_TTL_SECONDS)
    
    status_code = rpc_response.get('code', 500)
    return jsonify(rpc_response.get('data', {"error": rpc_response.get('error')})), status_code

@app.route('/record', methods=['POST'])
def add_record():
    print(f"\n--- AppNode-{app.port}: Received request for /record ---")
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400
    
    payload = request.get_json()
    print(f"[*] AppNode-{app.port}: Payload: {payload}")
    rpc_payload = {"action": "add_record", "data": payload}
    rpc_response = send_rpc_to_data_node(rpc_payload)
    
    # WRITE-THROUGH: Invalidate cache after successful DB write
    if redis_client and rpc_response.get('status') == 'success':
        patient_uuid = payload.get('patient_uuid')
        if patient_uuid:
            cache_key_to_invalidate = f"records:{patient_uuid}"
            print(f"[*] Cache Invalidation: Deleting key '{cache_key_to_invalidate}'.")
            redis_client.delete(cache_key_to_invalidate)

    status_code = rpc_response.get('code', 500)
    return jsonify(rpc_response), status_code

@app.route('/records/<string:patient_uuid>', methods=['GET'])
def get_records(patient_uuid):
    print(f"\n--- AppNode-{app.port}: Received request for /records/{patient_uuid} ---")
    
    # CACHE LOGIC: Check cache first for this patient's records
    cache_key = f"records:{patient_uuid}"
    if redis_client:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            print(f"[*] Cache Hit for key: '{cache_key}'")
            return jsonify(json.loads(cached_data)), 200
            
    print(f"[*] Cache Miss for key: '{cache_key}'. Fetching from DataNode.")
    payload = {"uuid": patient_uuid}
    rpc_payload = {"action": "get_records_by_uuid", "data": payload}
    rpc_response = send_rpc_to_data_node(rpc_payload)
    
    # CACHE LOGIC: Populate cache on successful DB read
    if redis_client and rpc_response.get('status') == 'success':
        data_to_cache = rpc_response.get('data')
        # We cache even empty lists to prevent repeated DB lookups for patients with no records
        if data_to_cache is not None:
            print(f"[*] Populating cache for key: '{cache_key}' with TTL {CACHE_TTL_SECONDS}s.")
            redis_client.set(cache_key, json.dumps(data_to_cache), ex=CACHE_TTL_SECONDS)
    
    status_code = rpc_response.get('code', 500)
    return jsonify(rpc_response.get('data', {"error": rpc_response.get('error')})), status_code

@app.route('/patients', methods=['GET'])
def get_all_patients_legacy():
    print(f"\n--- AppNode-{app.port}: Received LEGACY request for /patients ---")
    
    # CACHE LOGIC: Check cache for the list of all patients
    cache_key = "all_patients"
    if redis_client:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            print(f"[*] Cache Hit for key: '{cache_key}'")
            return jsonify(json.loads(cached_data)), 200
            
    print(f"[*] Cache Miss for key: '{cache_key}'. Fetching from DataNode.")
    rpc_payload = { "action": "get_all_patients", "data": {} }
    response = send_rpc_to_data_node(rpc_payload)
    
    # CACHE LOGIC: Populate cache on successful DB read
    if redis_client and response.get("status") == "success":
        data_to_cache = response.get("data")
        if data_to_cache:
            print(f"[*] Populating cache for key: '{cache_key}' with TTL {CACHE_TTL_SECONDS}s.")
            redis_client.set(cache_key, json.dumps(data_to_cache), ex=CACHE_TTL_SECONDS)
    
    status_code = 200 if response.get("status") == "success" else 500
    return jsonify(response.get("data", [])), status_code

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python app_node.py <port>")
        sys.exit(1)
    
    app.port = int(sys.argv[1])
    print(f"Application Node is running on http://127.0.0.1:{app.port}")
    app.run(port=app.port, debug=False)