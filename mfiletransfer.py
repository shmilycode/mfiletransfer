import threading
import socketserver
import socket
import argparse
import json
import time
from enum import Enum

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
  pass

class ThreadedRequestHandler(socketserver.BaseRequestHandler):
  def setup(self):
    self.client_address = self.client_address[0].strip()
    self.client_port = self.client_address[1]
    self.setup_handler(self.client_address, self.client_port)
    print("Client from %s:%s" % (self.client_address, self.client_port))

  def setup_handler(self, address, port):
    pass

  def handle(self):
    while True:
      data = str(self.request.recv(1024))
      if len(data):
        print("receve %d data" % len(data))
        self.data_handler(data);
      time.sleep(0.1)
  
  def data_handler(self, data):
    pass
  
  def finish(self):
    print("%s:%d disconnect!"%(self.client_address, self.client_port))

class CommandRequestHandler(ThreadedRequestHandler):
  def __init__(self):
    ThreadedRequestHandler.__init__(self)
    self.command_handler = CommandHandler.instance()
  
  def setup_handler(self, address, port):
    self.command_handler.register(address, port)
  
  def data_handler(self, data):
    self.command_handler.parse_client_request(data);

class CommandClient:
  def __init__(self, command_handler):
    self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.command_handler = command_handler

  def __del__(self):
    self.socket.close()

  def connect(self, server_address, server_port):
    self.server_address = server_address
    self.server_port = server_port
#    self.socket.settimeout(4)
    try:
      self.socket.connect((self.server_address, self.server_port))
    except socket.error:
      print("connect failed, error: %s" % socket.error)
      return False

    return True

  def wait_transfer_start(self):
    while True:
      data = self.socket.recv(1024)
      if data:
        result = self.command_handler.parse_server_request(data)
        if not result["name"]:
          continue
        self.file_name = reseult["name"]
        self.file_checksum = result["checksum"]
        self.file_size = result["size"]
        break
  
  def retransmission_request(self, packet_list):
    packet = self.command_handler.retransmission_request(packet_list)
    self.socket.sendall(packet)
  
  def block_request(self, block_index):
    packet = self.command_handler.block_request(block_index)

class CommandTypeEnum(Enum):
  BlockAcknowledge = 0,
  Retransmission = 1


# Singletone class
class CommandHandler(object):
  _instance_lock = threading.Lock()

  def __init__(self):
    self.address_group = []

  def __new__(cls, *args, **kwargs):
    if not hasattr(CommandHandler, "_instance"):
      with CommandHandler._instance_lock:
        if not hasattr(CommandHandler, "_instance"):
          CommandHandler._instance = object.__new__(cls)
    return CommandHandler._instance
  
  def register(self, address, port):
    self.address_group.append((address, port))

  def parse_server_request(self, data):
    jresp = json.load(data)
    print(jresp)
    return jresp
  
  def transfer_start_request(self, name, checksum, size):
    jresp = {"name": name, "checksum": checksum, "size": size}
    print(jresp)
    return json.dumps(jresp)
  
  def parse_client_request(self, data):
    jresp = json.load(data)
    print(jresp)
    return jresp

  def retransmission_request(self, packet_list):
    list_str = ",".join(str(id) for id in packet_list)
    jresp = [{'type': str(CommandTypeEnum.BlockAcknowledge),
              'slice_list': list_str}]
    return json.dumps(jresp)

if __name__ == "__main__":
  arg_parser = argparse.ArgumentParser(description="manual to this script")
  arg_parser.add_argument('-c', "--client", help="run in client mode",
                          action="store_true")
  arg_parser.add_argument('-s', "--server", help="run in server mode",
                          action="store_true")
  arg_parser.add_argument('-a', "--address", help="server address", 
                          type=str)
  arg_parser.add_argument('-p', "--port", help="server port", 
                          type=int)
  arg_parser.add_argument('-f', "--name", help="file path", 
                          type=str)
  arg_parser.add_argument('-F', "--path_to_save", help="path to save file which was recevied from server ", 
                          type=str)


  args = arg_parser.parse_args()

  if args.client:
    if not args.address or not args.port:
      print("Client mode need server address and port!")
      arg_parser.print_help()
    command_client = CommandClient(CommandHandler())
    if command_client.connect(args.address, args.port):
#      command_client.wait_transfer_start()
      # for test
      command_client.retransmission_request([1,2,3])
      print("Get start signal")
  elif args.server:
    host = args.address if args.address else "localhost"
    port = args.port if args.port else 60000
    server = ThreadedTCPServer((host, port), ThreadedRequestHandler)
    ip, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    print("Server %s:%d loop running in thread: %s"%(ip, port, server_thread.name))
    server_thread.join()
#    server.shutdown()
#    server.server_close()