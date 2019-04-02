import threading
import socket
import select
import argparse
import json
import sys
import logging
import struct
import os
import functools
import time

MAX_LISTEN_COUNT = 60
MAX_UDP_PACKET_SIZE = 1472
DEFAULT_BLOCK_SIZE = 1*1024*1024
DEFAULT_SLICE_SIZE = MAX_UDP_PACKET_SIZE - 4

def baseN(num, b):
    return ((num == 0) and "0") or \
           (baseN(num // b, b).lstrip("0") + "0123456789abcdefghijklmnopqrstuvwxyz"[num % b])


class MFileTransferServer:
  def __init__(self, port, command_handler):
    self.port = port
    self.command_handler = command_handler
    self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server_socket.bind(("localhost", port))
    self.server_socket.listen(socket.SOMAXCONN)
    self.descriptors = [self.server_socket,]
    self.retransmission_set = {}
    self.response_client_list = {}
  
  def server_address(self):
    return self.server_socket.getsockname()
  
  def run(self):
    while True:
      (sread, swrite, sexc) = select.select(self.descriptors, [], [])
      for client in sread:
        if client == self.server_socket:
          self.accept_new_connection()
        else:
          data = client.recv(1024)
          if len(data):
            self.request_handler(client, data)
          else:
            logging.info("%s:%s client disconnected!" % client.getpeername())
            client.close()
            self.descriptors.remove(client)

  def request_handler(self, client, data):
    request = self.command_handler.parse_client_request(data)
    if request['type'] == CommandHandler.RetransmissionRequest:
      if request['slice_list'] != None:
        slice_list.append(int(slice) for slice in request['slice_list'].split(','))
        self.retransmission_set.add(item for item in slice_list)



  def accept_new_connection(self):
    new_client, (remote_host, remote_port) = self.server_socket.accept()
    self.descriptors.append(new_client)
    logging.info("%s:%s client connected!" % new_client.getpeername())
  
  def broadcast(self, data, ignore_clients=[]):
    try:
      for client in self.descriptors:
        if client != self.server_socket and (client not in ignore_clients):
          client.sendall(data)
    except OSError as err:
      logging.error(err)

  def transfer_start_request(self, name, size, block_size, slice_size):
    request = self.command_handler.build_transfer_start_request(
      name, size, block_size, slice_size)
    self.broadcast(request)

  def need_block_complete_confirm(self, flush):
    self.retransmission_set = {}
    self.response_client_list = {}

  # may block here, because we have to wait for all clients response
  def get_retransmission_slices(self):
    # send block complete confirm, waitting for retransmission
    request = self.command_handler.build_block_complete_confirm()
    self.broadcast(request)
    while len(self.response_client_list) != (len(self.descriptors) - 1):
      logging.debug("Waitting for all clients to response")
      time.sleep(0.1)
    return list(self.retransmission_set)


class MFileTransferClient:
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
        if result["type"] != CommandHandler.TransferStartNotify:
          continue
        return result["name"],result["size"],result["block_size"],result["slice_size"]
    
  def wait_server_command(self):
     while True:
      data = self.socket.recv(1024)
      if data:
        result = self.command_handler.parse_server_request(data.decode('utf-8'))
        if result["type"] == CommandHandler.TransferCompleteConfirm:
          return False
        elif result["type"] == CommandHandler.BlockCompleteConfirm:
          return True

  def retransmission_request(self, packet_list):
    packet = self.command_handler.build_retransmission_request(packet_list)
    self.socket.sendall(packet)
  
  def block_request(self, block_index):
    packet = self.command_handler.block_request(block_index)

# Singletone class
class CommandHandler(object):
  _instance_lock = threading.Lock()
  BlockCompleteConfirm = 0
  RetransmissionRequest = 2
  TransferCompleteConfirm = 3
  TransferStartNotify = 4

  def __init__(self):
    self.address_group = []

  def __new__(cls, *args, **kwargs):
    if not hasattr(CommandHandler, "_instance"):
      with CommandHandler._instance_lock:
        if not hasattr(CommandHandler, "_instance"):
          CommandHandler._instance = object.__new__(cls)
    return CommandHandler._instance

  def parse_server_request(self, data):
    jresp = json.loads(data)
    logging.debug(jresp)
    return jresp
  
  def build_transfer_start_request(self, name, size, block_size, slice_size):
    jresp = {"type": CommandHandler.TransferStartNotify, "name": name, "size": size, 
             "block_size": block_size, "slice_size": slice_size}
    logging.debug(jresp)
    return bytes(json.dumps(jresp).encode('utf-8'))
  
  def build_block_complete_confirm(self):
    jresp = {"type": CommandHandler.BlockCompleteConfirm}
    logging.debug(jresp)
    return bytes(json.dumps(jresp).encode('utf-8'))

  def build_transfer_complete_confirm(self):
    jresp = {"type": CommandHandler.BlockCompleteConfirm}
    logging.debug(jresp)
    return bytes(json.dumps(jresp).encode('utf-8'))

  def build_retransmission_request(self, packet_list):
    jresp = {'type': CommandHandler.RetransmissionRequest}
    if len(packet_list):
      list_str = ",".join(str(id) for id in packet_list)
      jresp['slice_list'] = list_str

    logging.debug(jresp)
    return bytes(json.dumps(jresp).encode('utf-8'))

  def parse_client_request(self, data):
    jresp = json.loads(data.decode('utf-8'))
    logging.debug(jresp)
    return jresp

class MulticastBroker:
  def __init__(self, multicast_address):
    self.multicast_host, self.multicast_port = multicast_address
    self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.multicast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.multicast_socket.bind(('', self.multicast_port))
    # default
    # self.multicast_socket.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_TTL, 20)
    # self.multicast_socket.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_LOOP, 1)
    # self.multicast_socket.setsockopt(socket.SOL_IP, socket.IP_MULTICAST_IF,
    #                     socket.inet_aton(intf) + socket.inet_aton('0.0.0.0'))
    # join group
    group = socket.inet_aton(self.multicast_host)
    mreq = struct.pack('4sL', group, socket.INADDR_ANY)
    self.multicast_socket.setsockopt(socket.SOL_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    self.data_handler = self.default_data_handler

  def default_data_handler(self, data):
    pass
  
  def set_data_handler(self, handler):
    self.data_handler = handler

  def receive_loop(self):
    while True:
      data = self.multicast_socket.recv(MAX_UDP_PACKET_SIZE)
      if len(data):
        if(self.data_handler(data) == False):
          logging.debug("Exit recve_loop")
          break;
  
  def send(self, data):
    if len(data) >= MAX_UDP_PACKET_SIZE:
      logging.warning("Data send by multicast should less than %d", MAX_UDP_PACKET_SIZE)
    self.multicast_socket.sendto(data, (self.multicast_host, self.multicast_port))

class FileBlock:
  def __init__(self, name):
    self.file_name = name
  
  def block_iterator(self, block_size):
    self.block_size = block_size
    with open(self.file_name, 'rb') as file:
      data = file.read(self.block_size)
      if len(data):
        yield data
      else:
        return

class MFileTransfer:
  def __init__(self, broker):
    self.broker = broker
    self.slice_buffer = {}
    self.current_slice_index = 0

  # slice data include a  4 bytes header, use to store index of slice.
  def build_slice(self, slice_data, slice_index):
    slice_header = struct.pack('I', slice_index)
    return slice_header + slice_data

  def send_block(self, block, slice_size):
    slice_count = int((len(block)-1)/slice_size) + 1
    logging.debug("Block size %d, slice count %d", len(block), slice_count)
    for block_slice_index in range(slice_count):
      file_slice = block[block_slice_index*slice_count:(block_slice_index+1)*slice_count]
      slice = self.build_slice(file_slice, self.current_slice_index)
      self.broker.send(slice)
      self.current_slice_index += 1

  def file_transfer(self, server, file_path):
    if not os.path.exists(file_path):
      logging.error("file %s not exist!", file_path)
      return

    file_size = os.path.getsize(file_path)
    max_block_size = DEFAULT_BLOCK_SIZE
    max_slice_size = DEFAULT_SLICE_SIZE
    # notify all client to receive file.
    server.transfer_start_request(file_path, file_size, max_block_size, max_slice_size)
    file_block = FileBlock(file_path)
    for block in file_block.block_iterator(max_block_size):
      # send block to client
      self.send_block(block, max_slice_size)
      # ask client for response
      server.need_block_complete_confirm(True)
      # retransmit all losing slice
      while True:
        retransmission_slices = server.get_retransmission_slices()
        if not len(retransmission_slices):
          break;
        self.retransmit(retransmission_slices, block, slice_size)

  def retransmit(self, slices, block, slice_size):
    for slice_index in slices:
      slice_start = slice_size*slice_index
      slice_end = slice_start+slice_size
      slice_end = slice_end if slice_end <= len(block) else len(block)-slice_start
      slice = self.build_slice(block[slice_start:slice_end], slice_index)
      self.broker.send()

  def save_file(self, client, path_to_save, file_info):
    file_name,file_size,block_size,slice_size = file_info
    slice_count = int((block_size-1) / slice_size) + 1
    if not os.path.exists(path_to_save) or os.path.isdir(path_to_save):
      logging.error("path \"%s\" not existe or not directory, saving to current directory %s."%(path_to_save,file_name))
      path_to_save = file_name
    else:
      path_to_save = path_to_save + '/' + file_name
      logging.info("Saving file to %s"%(path_to_save))
    # receive block complete confirm request
    with open(path_to_save) as saved_file:
      while True:
        if True == client.wait_server_command():
          missing_slice = self.get_missing_slice(slice_count) 
          logging.debug("missing slice " + str(missing_slice))
          if not len(missing_slice):
            self.sync_data(saved_file, slice_count)
          client.retransmission_request(missing_slice)
        else: #receive file transfer complete confirm request
          left_file_size = file_size - (self.current_slice_index)*slice_size
          logging.debug("left file size: %d", left_file_size)
          slice_count = int((left_file_size-1)/slice_size + 1)
          missing_slice = self.get_missing_slice(slice_count) 
          client.retransmission_request(missing_slice)
          if not len(missing_slice):
            self.sync_data(saved_file, slice_count)
            break

  def get_missing_slice(self, slice_count):
    missing_slices = []
    for slice_index in range(self.current_slice_index, self.current_slice_index+slice_count):
      if slice_index not in self.slice_buffer:
        missing_slices.append(slice_index)
    return missing_slices
  
  def sync_data(self, file, slice_count):
    logging.debug("curren_slice_index: %d, slice_count: %d"%(self.current_slice_index, slice_count))
    for slice_index in range(self.current_slice_index, self.current_slice_index+slice_count):
      file.write(self.slice_buffer[slice_index])
    self.current_slice_index = self.current_slice_index + slice_count
    self.slice_buffer = {}

  def receive_slice_handler(self, data):
    slice_header = data[0:4]
    slice_index, = struct.unpack('I', slice_header)
    logging.debug("Receive slice %d" % slice_index)
    if slice_index > self.current_slice_index and (slice_index not in self.slice_buffer):
      self.slice_buffer = {slice_index: data[4:]}


def input_handler(input, server, transfer):
  file_path = input
  return transfer.file_transfer(server, file_path)

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
  arg_parser.add_argument('-l', "--listen_port", help="port to listen for server", 
                          type=int)
  arg_parser.add_argument('-f', "--name", help="file path", 
                          type=str)
  arg_parser.add_argument('-F', "--path_to_save", help="path to save file which was recevied from server ", 
                          type=str)
  arg_parser.add_argument('-m', "--multicast_address", help="address to send for receive file through multicast ", 
                          type=str)

  args = arg_parser.parse_args()

  if not args.multicast_address or args.multicast_address.find(':') == -1:
    multicast_ip = "225.100.100.6"
    multicast_port  = "5555"
    logging.info("Using default muticast address %s:%s", multicast_ip, multicast_port)
  else:
    multicast_ip,multicast_port = args.multicast_address.split(":")

  sender_broker = MulticastBroker((multicast_ip, int(multicast_port)))
  transfer = MFileTransfer(sender_broker)

  if args.client:
    if not args.address:
      logging.critical("Client mode need server address like \"127.0.0.1:60001\"")
      arg_parser.print_help()
      exit()

    server_ip, server_port = args.address.split(':')
    command_client = MFileTransferClient(CommandHandler())
    if command_client.connect(server_ip, int(server_port)):
      file_info = command_client.wait_transfer_start()
      logging.debug("Get start signal")
      sender_broker.set_data_handler(transfer.receive_slice_handler)
      receive_thread = threading.Thread(target=sender_broker.receive_loop)
      receive_thread.daemon = True
      receive_thread.start()

      path_to_save = args.path_to_save if args.path_to_save else "tmp"
      transfer.save_file(command_client, path_to_save, file_info)

      receive_thread.join()
  elif args.server:
    port = args.listen_port if args.listen_port else 60000
    server = MFileTransferServer(port, CommandHandler())
    server_ip, server_port = server.server_address()

    server_thread = threading.Thread(target=server.run)
    server_thread.daemon = True
    server_thread.start()
    logging.info("Server %s:%d loop running in thread: %s"%(server_ip, server_port, server_thread.name))

    for line in sys.stdin:
      is_exit = input_handler(line.strip('\n'), server, transfer)
      if is_exit:
        break;

    server_thread.join()
#    server.shutdown()
#    server.server_close()