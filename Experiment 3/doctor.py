from xmlrpc.client import ServerProxy

def create_proxy(host='localhost', port=3000):
    """Call the RPC server to add two numbers."""
    proxy = ServerProxy(f'http://{host}:{port}')
    return proxy


if __name__ == '__main__':
    remote  = create_proxy()
    while(1==1):
        print("Please enter Patient Id")
        Pid = input()
        print(remote.fetch_person(Pid))
