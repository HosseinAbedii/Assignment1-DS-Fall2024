# client-code.py
import socket
import json
import sys
import os
from typing import Dict, List

class DataClient:
    def __init__(self, host: str = 'localhost', port: int = 5000):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.data: List[Dict] = []
        self.client_id = None
        self.load_client_id()
        
    def load_client_id(self):
        """بارگذاری شناسه کلاینت از فایل"""
        try:
            if os.path.exists('client_id.txt'):
                with open('client_id.txt', 'r') as f:
                    self.client_id = f.read().strip()
                print(f"Loaded stored client ID: {self.client_id}")
        except Exception as e:
            print(f"Error loading client ID: {e}")

    def save_client_id(self, client_id: str):
        """ذخیره شناسه کلاینت در فایل"""
        try:
            with open('client_id.txt', 'w') as f:
                f.write(client_id)
            self.client_id = client_id
            print(f"Saved client ID: {client_id}")
        except Exception as e:
            print(f"Error saving client ID: {e}")
        
    def connect(self):
        """اتصال به سرور"""
        try:
            self.socket.connect((self.host, self.port))
            print("Connected to server")
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

    def receive_all(self):
        """دریافت کامل داده از سرور"""
        BUFF_SIZE = 4096
        data = b''
        while True:
            part = self.socket.recv(BUFF_SIZE)
            data += part
            if len(part) < BUFF_SIZE:
                break
        return data.decode()
            
    def request_data(self):
        """درخواست داده از سرور"""
        try:
            request = {
                'type': 'get_data'
            }
            self.socket.send(json.dumps(request).encode())
            
            response = json.loads(self.receive_all())
            if response['type'] == 'data_response':
                self.data = response['data']
                if 'client_id' in response and response['client_id'] != self.client_id:
                    self.save_client_id(response['client_id'])
                
                print("\nReceived data:")
                if not self.data:
                    print("No data available")
                else:
                    for record in self.data:
                        print("\nRecord:")
                        for key, value in record.items():
                            print(f"{key}: {value}")
                        print("-" * 50)
        except Exception as e:
            print(f"Error requesting data: {e}")
            
    def request_id_locations(self):
        """درخواست موقعیت رکوردها"""
        try:
            request = {
                'type': 'get_id_locations'
            }
            self.socket.send(json.dumps(request).encode())
            
            response = json.loads(self.receive_all())
            if response['type'] == 'id_locations_response':
                if 'client_id' in response and response['client_id'] != self.client_id:
                    self.save_client_id(response['client_id'])
                    
                print("\nRecord Locations:")
                if not response['data']:
                    print("No records currently allocated")
                else:
                    for record_id, client in response['data'].items():
                        print(f"Record {record_id} is assigned to {client}")
        except Exception as e:
            print(f"Error requesting record locations: {e}")
            
    def disconnect(self):
        """قطع اتصال از سرور"""
        try:
            self.socket.close()
            print("Disconnected from server")
        except Exception as e:
            print(f"Error disconnecting: {e}")
            
    def start_interactive(self):
        """شروع جلسه تعاملی"""
        if not self.connect():
            return
            
        while True:
            print("\nOptions:")
            print("1. Request data")
            print("2. Show record locations")
            print("3. Disconnect")
            print(f"Current Client ID: {self.client_id or 'Not yet assigned'}")
            
            try:
                choice = input("Enter choice (1-3): ")
                
                if choice == '1':
                    self.request_data()
                elif choice == '2':
                    self.request_id_locations()
                elif choice == '3':
                    self.disconnect()
                    break
                else:
                    print("Invalid choice")
            except KeyboardInterrupt:
                self.disconnect()
                break

if __name__ == "__main__":
    client = DataClient()
    client.start_interactive()