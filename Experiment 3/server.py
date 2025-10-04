import sqlite3
from xmlrpc.server import SimpleXMLRPCServer

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
        cursor.execute("INSERT INTO patient (name, age) VALUES (?, ?) RETURNING pid;", (name, age))
        pid = cursor.fetchone()
        conn.commit()
        return(f"Successfully added '{name}' (Age: {age}) to the database. Pid is {pid}")

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
        if(len(res)>0):
            return(res[0])
        else:
            return "No Record found"

    except sqlite3.Error as e:
        return(f"Error adding person to database: {e}")

    finally:
        if conn:
            conn.close()



def list_rpc_functions(host='localhost', port=3000):
    """Starts the server that listens for incoming RPC requests."""
    server = SimpleXMLRPCServer((host,port))
    server.register_function(insert_person)
    server.register_function(fetch_person)
    return server

if __name__ == '__main__':
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS patient (
                    pid INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    age INTEGER NOT NULL
                    );""")
    
    conn.commit()
    conn.close()

    server = list_rpc_functions()
    try:
        print('Server started')
        server.serve_forever()
    except KeyboardInterrupt:
        print('exiting')
