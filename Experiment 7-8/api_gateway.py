from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
from itertools import cycle

# --- Configuration ---
# The addresses of our Application Node instances.
# In a real-world scenario (e.g., using Docker or Kubernetes),
# these would be service names.
APP_NODE_URLS = [
    "http://127.0.0.1:6001",
    "http://127.0.0.1:6002",
    "http://127.0.0.1:6003",
]

# --- Initialize Flask App and Load Balancer ---
app = Flask(__name__)
CORS(app)

# Use itertools.cycle for a simple and effective round-robin load balancer.
# This will cycle through the APP_NODE_URLS list indefinitely.
app_node_cycler = cycle(APP_NODE_URLS)

# --- Generic Proxy/Forwarding Route ---
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def proxy_request(path):
    """
    This is the core of the API Gateway. It captures all incoming requests
    and forwards them to one of the backend Application Nodes.
    """
    # 1. Select the next Application Node from our cycle.
    target_node_url = next(app_node_cycler)
    
    # Construct the full URL for the target service.
    url = f"{target_node_url}/{path}"
    
    print(f"API Gateway forwarding request for '{path}' to {target_node_url}")

    try:
        # 2. Forward the request.
        # We use requests.request to dynamically handle any HTTP method.
        # We also pass along the headers, form data, and JSON data.
        response = requests.request(
            method=request.method,
            url=url,
            headers={key: value for (key, value) in request.headers if key != 'Host'},
            data=request.get_data(),
            cookies=request.cookies,
            allow_redirects=False,
            timeout=5 # Add a timeout for resilience
        )

        # 3. Return the response from the Application Node back to the original client.
        # We create a response object with the content, status code, and headers
        # from the backend service's response.
        headers = [(name, value) for (name, value) in response.raw.headers.items()]
        return response.content, response.status_code, headers

    except requests.exceptions.RequestException as e:
        print(f"Error forwarding request to {target_node_url}: {e}")
        return jsonify({"error": "Service unavailable"}), 503


if __name__ == '__main__':
    # The gateway runs on port 5000, which is what the frontend expects.
    print("API Gateway is running on http://127.0.0.1:5000")
    app.run(port=5000, debug=True)