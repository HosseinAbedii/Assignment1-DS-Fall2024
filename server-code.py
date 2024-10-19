import socket
import threading
import json
import random
import csv
from typing import Dict, List
import errno

class DataDistributionServer:
    def __init__(self, host: str = 'localhost', port: int = 5000):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.clients: Dict[str, socket.socket] = {}
        self.client_data: Dict[str, List[Dict]] = {}
        self.available_data: List[Dict] = []
        self.lock = threading.Lock()
        self.running = True

    def load_data(self, filename: str):
        """Load and parse CSV data"""
        try:
            with open(filename, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                self.available_data = list(csv_reader)
                print(f"Loaded {len(self.available_data)} records from {filename}")
        except Exception as e:
            print(f"Error loading data: {e}")
            raise

    def is_socket_closed(self, sock: socket.socket) -> bool:
        """Check if a socket is closed or in error state"""
        try:
            # Return True if socket is closed or in error state
            return sock.fileno() == -1
        except Exception:
            return True

    def handle_client(self, client_socket: socket.socket, client_id: str):
        """Handle individual client connections"""
        print(f"New client connected: {client_id}")
        
        try:
            while self.running:
                try:
                    data = client_socket.recv(4096).decode()
                    if not data:
                        print(f"Client {client_id} disconnected gracefully")
                        break

                    request = json.loads(data)
                    
                    if request['type'] == 'get_data':
                        client_data = self.distribute_data(client_id)
                        response = {
                            'type': 'data_response',
                            'data': client_data
                        }
                        if not self.is_socket_closed(client_socket):
                            client_socket.send(json.dumps(response).encode())
                        
                    elif request['type'] == 'get_id_locations':
                        id_locations = {}
                        for cid, data in self.client_data.items():
                            for record in data:
                                id_locations[record['id']] = cid
                        response = {
                            'type': 'id_locations_response',
                            'data': id_locations
                        }
                        if not self.is_socket_closed(client_socket):
                            client_socket.send(json.dumps(response).encode())

                except (ConnectionResetError, ConnectionAbortedError) as e:
                    print(f"Client {client_id} connection reset/aborted: {e}")
                    break
                except socket.error as e:
                    if e.errno == errno.ECONNRESET:
                        print(f"Client {client_id} forcibly disconnected")
                    else:
                        print(f"Socket error for client {client_id}: {e}")
                    break
                except json.JSONDecodeError:
                    print(f"Invalid JSON received from client {client_id}")
                    break
                except Exception as e:
                    print(f"Unexpected error handling client {client_id}: {e}")
                    break

        finally:
            self.cleanup_client(client_id, client_socket)

    def cleanup_client(self, client_id: str, client_socket: socket.socket):
        """Clean up client resources and redistribute data"""
        print(f"Cleaning up resources for client {client_id}")
        try:
            with self.lock:
                if client_id in self.clients:
                    if not self.is_socket_closed(client_socket):
                        client_socket.close()
                    del self.clients[client_id]
                    self.redistribute_data(client_id)
        except Exception as e:
            print(f"Error during cleanup for client {client_id}: {e}")

    def distribute_data(self, client_id: str) -> List[Dict]:
        """Randomly distribute available data to a client"""
        with self.lock:
            if not self.available_data:
                return []
                
            n_records = max(1, len(self.available_data) // (len(self.clients) + 1))
            selected_indices = random.sample(range(len(self.available_data)), 
                                          min(n_records, len(self.available_data)))
            selected_data = [self.available_data[i] for i in sorted(selected_indices, reverse=True)]
            
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
            redistrib_data = self.client_data.pop(disconnected_client_id)
            self.available_data.extend(redistrib_data)
            
            if self.clients:
                for client_id in list(self.clients.keys()):
                    if client_id in self.clients:  # Check again in case client disconnected
                        try:
                            new_data = self.distribute_data(client_id)
                            if new_data:
                                self.send_data_to_client(client_id, new_data)
                        except Exception as e:
                            print(f"Error redistributing to client {client_id}: {e}")

    def send_data_to_client(self, client_id: str, data: List[Dict]):
        """Send data to a specific client"""
        try:
            if client_id in self.clients and not self.is_socket_closed(self.clients[client_id]):
                message = {
                    'type': 'data_update',
                    'data': data
                }
                self.clients[client_id].send(json.dumps(message).encode())
        except Exception as e:
            print(f"Error sending data to client {client_id}: {e}")

    def start(self):
        """Start the server"""
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            print(f"Server listening on {self.host}:{self.port}")
            
            while self.running:
                try:
                    client_socket, addr = self.server_socket.accept()
                    client_id = f"client_{len(self.clients) + 1}"
                    self.clients[client_id] = client_socket
                    
                    thread = threading.Thread(target=self.handle_client, 
                                           args=(client_socket, client_id))
                    thread.daemon = True
                    thread.start()
                except Exception as e:
                    print(f"Error accepting client connection: {e}")
                    
        except KeyboardInterrupt:
            print("\nServer shutting down...")
        except Exception as e:
            print(f"Server error: {e}")
        finally:
            self.shutdown()

    def shutdown(self):
        """Shutdown the server and cleanup"""
        self.running = False
        print("Closing all client connections...")
        
        # Close all client connections
        with self.lock:
            for client_id, client_socket in list(self.clients.items()):
                try:
                    if not self.is_socket_closed(client_socket):
                        client_socket.close()
                except Exception as e:
                    print(f"Error closing client {client_id} connection: {e}")
            self.clients.clear()
            self.client_data.clear()
        
        # Close server socket
        try:
            if not self.is_socket_closed(self.server_socket):
                self.server_socket.close()
        except Exception as e:
            print(f"Error closing server socket: {e}")
        
        print("Server shutdown complete")

if __name__ == "__main__":
    server = DataDistributionServer()
    try:
        server.load_data('RandomData.csv')
        server.start()
    except Exception as e:
        print(f"Fatal error: {e}")
        server.shutdown()