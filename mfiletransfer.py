import threading
import socketserver  # python 3.x , use SocketServer for python2.x
import socket
import argparse
import json
import time
import sys
import logging
from enum import Enum

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
  def __init__(self, address, handler, command_handler):
    # python 3.x, use super(ThreadedTCPServer, self).init(address, handler) for python2.x
    super().__init__(address, handler)
    self.__my_shutdown = False
    self.command_handler = command_handler
    self.handler = handler

  def shutdown(self):
    super().shutdown()
    self.__my_shutdown = True
  
  def is_shutdown(self):
    return self.__my_shutdown;

class ThreadedRequestHandler(socketserver.BaseRequestHandler):
  def setup(self):
    (self.client_address,self.client_port) = self.client_address
    self.command_handler = self.server.command_handler
    self.request.settimeout(1)
    self.shutdown = False
    self.setup_handler(self.client_address, self.client_port)
    logging.info("Client from %s:%s" % (self.client_address, self.client_port))

  def setup_handler(self, address, port):
    self.command_handler.register(address, port)

  def handle(self):
    while not self.server.is_shutdown():
      try:
        data = self.request.recv(1024)
      except socket.timeout as timeout:
        continue;
      except OSError as err: 
        logging.error(err)
        break;

      if len(data):
        logging.debug("receve %d data" % len(data))
        self.data_handler(data);
      else:
        break;
      time.sleep(0.1)
  
  def data_handler(self, data):
    self.command_handler.parse_client_request(data.decode('utf-8'));

  def transfer_start_request(self, name, checksum, size):
    request = self.command_handler.build_retransmission_request(name, checksum, size)
    try:
      self.self.request.sendall(request)
    except OSError as err:
      logging.error(err)
  
  def finish(self):
    logging.info("%s:%s disconnect!"%(self.client_address, self.client_port))
    self.command_handler.unregister(self.client_address, self.client_port)

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
    except OSError as msg:
      logging.error("connect failed, error: %s" % msg)
      return False

    return True

  def wait_transfer_start(self):
    while True:
      data = self.socket.recv(1024)
      if data:
        result = self.command_handler.parse_server_request(data.decode('utf-8'))
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

# Singletone class
class CommandHandler(object):
  _instance_lock = threading.Lock()
  BlockAcknowledge = 0
  Retransmission = 1

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

  def unregister(self, address, port):
    self.address_group.remove((address, port));

  def parse_server_request(self, data):
    jresp = json.load(data)
    logging.debug(jresp)
    return jresp
  
  def build_transfer_start_request(self, name, checksum, size):
    jresp = {"name": name, "checksum": checksum, "size": size}
    logging.debug(jresp)
    return json.dumps(jresp)
  
  def parse_client_request(self, data):
    jresp = json.loads(data)
    logging.debug(jresp)
    return jresp

  def build_retransmission_request(self, packet_list):
    list_str = ",".join(str(id) for id in packet_list)
    jresp = {'type': CommandHandler.Retransmission,
              'slice_list': list_str}
    logging.debug(jresp)
    return bytes(json.dumps(jresp).encode('utf-8'))

def input_handler(input, server):
  if (input == 'exit'):
    server.shutdown()
    server.server_close()
    return True

if __name__ == "__main__":

  LOG_FORMAT = "[%(asctime)s:%(levelname)s:%(funcName)s]  %(message)s"
  logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT)

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
      logging.critical("Client mode need server address and port!")
      arg_parser.print_help()
    command_client = CommandClient(CommandHandler())
    if command_client.connect(args.address, args.port):
      command_client.wait_transfer_start()
      # for test
#      command_client.retransmission_request([1,2,3])
      logging.debug("Get start signal")
  elif args.server:
    host = args.address if args.address else "localhost"
    port = args.port if args.port else 60000
    server = ThreadedTCPServer((host, port), ThreadedRequestHandler, CommandHandler())
    ip, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.daemon = True
    server_thread.start()
    logging.info("Server %s:%d loop running in thread: %s"%(ip, port, server_thread.name))

    for line in sys.stdin:
      is_exit = input_handler(line.strip('\n'), server)
      if is_exit:
        break;

    server_thread.join()
#    server.shutdown()
#    server.server_close()