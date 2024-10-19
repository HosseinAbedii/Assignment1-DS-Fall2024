import socket
import threading
import json
import random
import csv
from typing import Dict, List, Set

class DataDistributionServer:
    def __init__(self, host: str = 'localhost', port: int = 5000):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.clients: Dict[str, socket.socket] = {}  # client_id -> socket
        self.client_data: Dict[str, List[Dict]] = {}  # client_id -> data records
        self.available_data: List[Dict] = []
        self.lock = threading.Lock()
        
    def load_data(self, filename: str):
        """Load and parse CSV data"""
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                self.available_data = list(csv_reader)
                print(f"Loaded {len(self.available_data)} records from {filename}")
        except Exception as e:
            print(f"Error loading data: {e}")
            self.available_data = []
            
    def send_json(self, socket: socket.socket, data: dict):
        """Send JSON data with length prefix"""
        try:
            json_data = json.dumps(data)
            socket.sendall(json_data.encode())
        except Exception as e:
            print(f"Error sending data: {e}")
            raise
            
    def distribute_data(self, client_id: str) -> List[Dict]:
        """Randomly distribute available data to a client"""
        with self.lock:
            if not self.available_data:
                return []
                
            # Randomly select ~1/n of remaining records where n is number of active clients
            n_records = max(1, len(self.available_data) // (len(self.clients) + 1))
            selected_indices = random.sample(range(len(self.available_data)), n_records)
            selected_data = [self.available_data[i] for i in sorted(selected_indices, reverse=True)]
            
            # Remove selected data from available pool
            for i in sorted(selected_indices, reverse=True):
                self.available_data.pop(i)
                
            self.client_data[client_id] = selected_data
            return selected_data
            
    def redistribute_data(self, disconnected_client_id: str):
        """Redistribute data from disconnected client to other clients"""
        with self.lock:
            if disconnected_client_id not in self.client_data:
                return
                
            print(f"Redistributing data from {disconnected_client_id}")
            self.available_data.extend(self.client_data[disconnected_client_id])
            del self.client_data[disconnected_client_id]
            
            # Redistribute to remaining clients
            if self.clients:
                records_per_client = len(self.available_data) // len(self.clients)
                for client_id in self.clients:
                    new_data = self.distribute_data(client_id)
                    if new_data:
                        response = {
                            'type': 'data_update',
                            'data': new_data
                        }
                        self.send_json(self.clients[client_id], response)
                        
    def handle_client(self, client_socket: socket.socket, client_id: str):
        """Handle individual client connections"""
        print(f"New client connected: {client_id}")
        
        try:
            while True:
                data = client_socket.recv(4096).decode()
                if not data:
                    break
                    
                request = json.loads(data)
                
                if request['type'] == 'get_data':
                    client_data = self.distribute_data(client_id)
                    response = {
                        'type': 'data_response',
                        'data': client_data
                    }
                    self.send_json(client_socket, response)
                    
                elif request['type'] == 'get_id_locations':
                    id_locations = {}
                    for cid, data in self.client_data.items():
                        for record in data:
                            id_locations[record['id']] = cid
                    response = {
                        'type': 'id_locations_response',
                        'data': id_locations
                    }
                    self.send_json(client_socket, response)
                    
        except Exception as e:
            print(f"Error handling client {client_id}: {e}")
        finally:
            print(f"Client disconnected: {client_id}")
            with self.lock:
                if client_id in self.clients:
                    del self.clients[client_id]
                    self.redistribute_data(client_id)
                    
    def start(self):
        """Start the server"""
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f"Server listening on {self.host}:{self.port}")
        
        try:
            while True:
                client_socket, addr = self.server_socket.accept()
                client_id = f"client_{len(self.clients) + 1}"
                self.clients[client_id] = client_socket
                
                thread = threading.Thread(target=self.handle_client, 
                                       args=(client_socket, client_id))
                thread.daemon = True
                thread.start()
        except KeyboardInterrupt:
            print("Server shutting down...")
        finally:
            self.server_socket.close()

if __name__ == "__main__":
    server = DataDistributionServer()
    server.load_data('RandomData.csv')
    server.start()