# client-code.py
import socket
import json
import sys
from typing import Dict, List

class DataClient:
    def __init__(self, host: str = 'localhost', port: int = 5000):
        self.host = host
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.data: List[Dict] = []
        
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
                print("\nReceived data:")
                if not self.data:
                    print("No data available")
                else:
                    for record in self.data:
                        # نمایش تمام فیلدهای موجود در رکورد
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