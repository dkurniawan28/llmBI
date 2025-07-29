import pymongo
import paramiko
from sshtunnel import SSHTunnelForwarder
import os

class MongoDBSSHConnection:
    def __init__(self):
        self.ssh_host = '103.93.56.51'
        self.ssh_port = 22
        self.ssh_username = 'ubuntu'
        self.ssh_key_path = '/Users/dedykurniawan/Downloads/pemFile/teh.pem'
        self.mongo_host = 'localhost'
        self.mongo_port = 27017
        self.db_name = 'esteh'
        self.tunnel = None
        self.client = None
        OPENROUTER_API_KEY="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"
    def connect(self):
        try:
            # Create SSH tunnel
            self.tunnel = SSHTunnelForwarder(
                (self.ssh_host, self.ssh_port),
                ssh_username=self.ssh_username,
                ssh_pkey=self.ssh_key_path,
                remote_bind_address=(self.mongo_host, self.mongo_port),
                local_bind_address=('localhost', 0)  # Use any available local port
            )
            
            self.tunnel.start()
            local_port = self.tunnel.local_bind_port
            
            # Connect to MongoDB through the tunnel
            self.client = pymongo.MongoClient(f'mongodb://localhost:{local_port}/')
            
            # Test connection
            self.client.admin.command('ping')
            print(f"Successfully connected to MongoDB via SSH tunnel on port {local_port}")
            
            return self.client
            
        except Exception as e:
            print(f"Connection failed: {e}")
            if self.tunnel:
                self.tunnel.stop()
            return None
    
    def disconnect(self):
        if self.client:
            self.client.close()
        if self.tunnel:
            self.tunnel.stop()
        print("Disconnected from MongoDB")
    
    def get_database(self, db_name=None):
        if not self.client:
            raise Exception("No active connection. Call connect() first.")
        
        db_name = db_name or self.db_name
        return self.client[db_name]

# Usage example
if __name__ == "__main__":
    mongo_conn = MongoDBSSHConnection()
    
    try:
        client = mongo_conn.connect()
        if client:
            # Get database
            db = mongo_conn.get_database()
            
            # List collections
            collections = db.list_collection_names()
            print(f"Collections in {mongo_conn.db_name}: {collections}")
            
            # Example query (uncomment to use)
            # collection = db['your_collection_name']
            # documents = collection.find().limit(5)
            # for doc in documents:
            #     print(doc)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        mongo_conn.disconnect()