import sys
import xmlrpc.client
from itertools import cycle
from flask import Flask, request, jsonify
from flask_cors import CORS

# --- Configuration ---
DATA_NODES = [
    ('127.0.0.1', 7001),
    ('127.0.0.1', 7002),
    ('127.0.0.1', 7003),
]
# We don't shuffle anymore to make round-robin predictable
data_node_cycler = cycle(DATA_NODES)

# --- Initialize Flask App ---
app = Flask(__name__)
CORS(app)

# --- XML-RPC Client Function ---
def send_rpc_to_data_node(rpc_message):
    """Sends a message to a Data Node using XML-RPC."""
    node_host, node_port = next(data_node_cycler)
    action = rpc_message['action']
    data = rpc_message['data']
    
    print(f"[*] AppNode-{app.port}: Forwarding action '{action}' to DataNode http://{node_host}:{node_port}")
    
    try:
        # Create a proxy to the data node's XML-RPC server
        proxy = xmlrpc.client.ServerProxy(f"http://{node_host}:{node_port}/", allow_none=True)
        
        # Call the remote 'dispatch_rpc' function with the action and its data
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

# --- API Endpoints (No changes needed here) ---

@app.route('/add_account', methods=['POST'])
def add_account():
    print(f"\n--- AppNode-{app.port}: Received request for /add_account ---")
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400
    
    print(f"[*] AppNode-{app.port}: Payload: {request.get_json()}")
    rpc_payload = {"action": "add_account", "data": request.get_json()}
    rpc_response = send_rpc_to_data_node(rpc_payload)
    
    status_code = rpc_response.get('code', 500)
    return jsonify(rpc_response), status_code

@app.route('/get_data/<string:username>', methods=['GET'])
def get_data(username):
    print(f"\n--- AppNode-{app.port}: Received request for /get_data/{username} ---")
    auth = request.authorization
    if not auth or not auth.username or not auth.password:
        return jsonify({"error": "Authentication required"}), 401

    print(f"[*] AppNode-{app.port}: Authenticating user '{auth.username}'")
    payload = {"username": auth.username, "password": auth.password}
    rpc_payload = {"action": "get_data", "data": payload}
    rpc_response = send_rpc_to_data_node(rpc_payload)
    
    status_code = rpc_response.get('code', 500)
    return jsonify(rpc_response.get('data', {"error": rpc_response.get('error')})), status_code

@app.route('/record', methods=['POST'])
def add_record():
    print(f"\n--- AppNode-{app.port}: Received request for /record ---")
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400
    
    print(f"[*] AppNode-{app.port}: Payload: {request.get_json()}")
    rpc_payload = {"action": "add_record", "data": request.get_json()}
    rpc_response = send_rpc_to_data_node(rpc_payload)
    
    status_code = rpc_response.get('code', 500)
    return jsonify(rpc_response), status_code

@app.route('/records/<string:patient_uuid>', methods=['GET'])
def get_records(patient_uuid):
    print(f"\n--- AppNode-{app.port}: Received request for /records/{patient_uuid} ---")
    payload = {"uuid": patient_uuid}
    rpc_payload = {"action": "get_records_by_uuid", "data": payload}
    rpc_response = send_rpc_to_data_node(rpc_payload)
    
    status_code = rpc_response.get('code', 500)
    return jsonify(rpc_response.get('data', {"error": rpc_response.get('error')})), status_code

@app.route('/patients', methods=['GET'])
def get_all_patients_legacy():
    print(f"\n--- AppNode-{app.port}: Received LEGACY request for /patients ---")
    rpc_payload = { "action": "get_all_patients", "data": {} }
    response = send_rpc_to_data_node(rpc_payload)
    status_code = 200 if response.get("status") == "success" else 500
    return jsonify(response.get("data", [])), status_code

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python app_node.py <port>")
        sys.exit(1)
    
    app.port = int(sys.argv[1])
    print(f"Application Node is running on http://127.0.0.1:{app.port}")
    app.run(port=app.port, debug=False)