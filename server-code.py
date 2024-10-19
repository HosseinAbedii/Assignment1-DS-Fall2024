# server-code.py
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
        """بارگذاری داده از فایل CSV"""
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                self.available_data = list(csv_reader)
                print(f"Loaded {len(self.available_data)} records from {filename}")
                if self.available_data:
                    print("Available columns:", list(self.available_data[0].keys()))
        except Exception as e:
            print(f"Error loading data: {e}")
            self.available_data = []

    def send_data(self, client_socket: socket.socket, data: dict):
        """ارسال داده به صورت امن"""
        try:
            json_data = json.dumps(data)
            message = json_data.encode()
            total_sent = 0
            while total_sent < len(message):
                sent = client_socket.send(message[total_sent:])
                if sent == 0:
                    raise RuntimeError("socket connection broken")
                total_sent += sent
        except Exception as e:
            print(f"Error sending data: {e}")
            
    def distribute_data(self, client_id: str) -> List[Dict]:
        """توزیع داده‌های موجود به کلاینت"""
        with self.lock:
            if not self.available_data:
                return []
                
            n_records = max(1, len(self.available_data) // (len(self.clients) + 1))
            selected_indices = random.sample(range(len(self.available_data)), min(n_records, len(self.available_data)))
            selected_data = [self.available_data[i] for i in sorted(selected_indices, reverse=True)]
            
            for i in sorted(selected_indices, reverse=True):
                self.available_data.pop(i)
                
            self.client_data[client_id] = selected_data
            return selected_data
            
    def redistribute_data(self, disconnected_client_id: str):
        """توزیع مجدد داده‌های کلاینت قطع شده"""
        with self.lock:
            if disconnected_client_id not in self.client_data:
                return
                
            print(f"Redistributing data from {disconnected_client_id}")
            self.available_data.extend(self.client_data[disconnected_client_id])
            del self.client_data[disconnected_client_id]
            
            if self.clients:
                for client_id in self.clients:
                    new_data = self.distribute_data(client_id)
                    if new_data:
                        response = {
                            'type': 'data_update',
                            'data': new_data
                        }
                        self.send_data(self.clients[client_id], response)
                        
    def handle_client(self, client_socket: socket.socket, client_id: str):
        """مدیریت اتصال کلاینت"""
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
                    self.send_data(client_socket, response)
                    
                elif request['type'] == 'get_id_locations':
                    id_locations = {}
                    # استفاده از اولین ستون به عنوان شناسه
                    for cid, data_list in self.client_data.items():
                        for record in data_list:
                            first_key = next(iter(record))
                            id_locations[record[first_key]] = cid
                    response = {
                        'type': 'id_locations_response',
                        'data': id_locations
                    }
                    self.send_data(client_socket, response)
                    
        except Exception as e:
            print(f"Error handling client {client_id}: {e}")
        finally:
            print(f"Client disconnected: {client_id}")
            with self.lock:
                if client_id in self.clients:
                    del self.clients[client_id]
                    self.redistribute_data(client_id)
                    
    def start(self):
        """راه‌اندازی سرور"""
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