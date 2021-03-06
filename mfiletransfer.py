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
import random
from subprocess import check_output
from ctypes import create_string_buffer

MAX_LISTEN_COUNT = 60
MAX_UDP_PACKET_SIZE = 1472
MAX_TCP_PACKET_SIZE = 4096
DEFAULT_BLOCK_SIZE = 5*1024*1024
DEFAULT_SLICE_SIZE = MAX_UDP_PACKET_SIZE - 4

def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
    finally:
        s.close()

    return ip

class MFileTransferServer:
  def __init__(self, address, command_handler):
    self.addres = address 
    self.command_handler = command_handler
    self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server_socket.bind(address)
    self.server_socket.listen(socket.SOMAXCONN)
    self.descriptors = [self.server_socket,]
    self.retransmission_set = set()
    self.response_client_list = set()
    self.client_buffer = {}
  
  def server_address(self):
    return self.server_socket.getsockname()
  
  def run(self):
    while True:
      (sread, swrite, sexc) = select.select(self.descriptors, [], [])
      for client in sread:
        if client == self.server_socket:
          self.accept_new_connection()
        else:
          try:
            data = client.recv(MAX_TCP_PACKET_SIZE)
            if len(data):
              # push into client_buffer
              self.client_buffer[client] += data
              self.request_handler(client)
            else:
              logging.info("%s:%s client disconnected!" % client.getpeername())
              client.close()
              self.descriptors.remove(client)
          except OSError as err:
            logging.error(err)
            client.close()
            self.descriptors.remove(client)
            del self.client_buffer[client]

  def request_handler(self, client):
    request = self.command_handler.parse_client_request(self.client_buffer[client])
    # failed to decode, fill to buffer
    if not request:
      return

    logging.debug(request)
    if request['type'] == CommandHandler.RetransmissionRequest:
      if 'slice_list' in request:
        slice_list = [int(slice) for slice in request['slice_list'].split(',')]
        self.retransmission_set.update({item for item in slice_list})

    self.client_buffer[client] = b''
    self.response_client_list.add(client)

  def accept_new_connection(self):
    new_client, (remote_host, remote_port) = self.server_socket.accept()
    self.descriptors.append(new_client)
    self.client_buffer[new_client] = b''
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
    self.retransmission_set = set()
    self.response_client_list = set()

  # may block here, because we have to wait for all clients response
  def get_retransmission_slices(self, block_index, last_block):
    # send block complete confirm, waitting for retransmission
    if not last_block:
      request = self.command_handler.build_block_complete_confirm(block_index)
    else:
      request = self.command_handler.build_transfer_complete_confirm(block_index)
    self.broadcast(request)
    while len(self.response_client_list) != (len(self.descriptors) - 1):
      logging.debug("Waitting for all clients to response")
      time.sleep(0.005)
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
      data = self.socket.recv(MAX_TCP_PACKET_SIZE)
      if len(data):
        result = self.command_handler.parse_server_request(data)
        if result["type"] == CommandHandler.TransferStartNotify:
          return result["name"],result["size"],result["block_size"],result["slice_size"]
        elif result["type"] == CommandHandler.TransferCompleteConfirm:
          logging.error("What, I don't know!")
          self.socket.sendall(self.command_handler.build_retransmission_request([]))
      else:
        break;
    
  def wait_server_command(self):
     while True:
      data = self.socket.recv(MAX_TCP_PACKET_SIZE)
      if len(data):
        result = self.command_handler.parse_server_request(data)
        if result["type"] == CommandHandler.TransferCompleteConfirm:
          return False,result["block"]
        elif result["type"] == CommandHandler.BlockCompleteConfirm:
          return True,result["block"]
      else:
        break;

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
    request_type = ord(data[:1])
    jresp = {}
    if request_type == CommandHandler.TransferStartNotify:
      name_len = ord(data[1:2])
      ttype,name_size,name,size,block_size,slice_size = struct.unpack('BB'+str(name_len)+'sQII', data)
      jresp = {"type": ttype, "name": name.decode('utf-8'), "size": size, 
               "block_size": block_size, "slice_size": slice_size}
    elif request_type == CommandHandler.BlockCompleteConfirm:
      rtype, block_index = struct.unpack('Bi', data)
      jresp = {"type": CommandHandler.BlockCompleteConfirm, "block":block_index}
    elif request_type == CommandHandler.TransferCompleteConfirm:
      rtype, block_index = struct.unpack('Bi', data)
      jresp = {"type": CommandHandler.TransferCompleteConfirm, "block":block_index}
    else:
      logging.error('Unkonw type %d'%(request_type))
    logging.debug(jresp)
    return jresp

  def build_transfer_start_request(self, name, size, block_size, slice_size):
    jresp = {"type": CommandHandler.TransferStartNotify, "name": name, "size": size, 
             "block_size": block_size, "slice_size": slice_size}
  
    bresp = struct.pack('BB'+str(len(name))+'sQII', CommandHandler.TransferStartNotify, len(name), name.encode('utf-8'),
                         size, block_size, slice_size)
    logging.debug(jresp)
    return bresp
  
  def build_block_complete_confirm(self, block_index):
    jresp = {"type": CommandHandler.BlockCompleteConfirm, "block":block_index}
    bresp = struct.pack('Bi', CommandHandler.BlockCompleteConfirm, block_index)
    logging.debug(jresp)
    return bresp

  def build_transfer_complete_confirm(self, block_index):
    jresp = {"type": CommandHandler.TransferCompleteConfirm}
    bresp = struct.pack('Bi', CommandHandler.TransferCompleteConfirm, block_index)
    logging.debug(jresp)
    return bresp

  def build_retransmission_request(self, packet_list):
    jresp = {'type': CommandHandler.RetransmissionRequest}
    if len(packet_list):
      list_str = ",".join(str(id) for id in packet_list)
      jresp['slice_list'] = list_str
    logging.debug(jresp)

    # Use two bytes to store one index, list end with 0xFFFF
    bresp = struct.pack('B', CommandHandler.RetransmissionRequest)
    list_size = (len(packet_list) + 1) * 2
    bresp = create_string_buffer(list_size+1)
    struct.pack_into('B', bresp, 0, CommandHandler.RetransmissionRequest)
    start_pos = 1

    if len(packet_list):
      for idx in packet_list:
        struct.pack_into('H', bresp, start_pos, idx)
        start_pos += 2

    struct.pack_into('H', bresp, start_pos, 0xffff)
    return bresp

  def parse_client_request(self, data):
    try:
      logging.debug("client request %d bytes"%len(data))
      end, = struct.unpack('H', data[-2:])
      if end == 0xffff:
        data_len = len(data)
        cur_pos = 1
        jresp = {'type': ord(data[:1])}
        data_list = []

        while cur_pos < data_len:
          idx, = struct.unpack_from('H', data, cur_pos)
          data_list.append(str(idx))
          cur_pos += 2
        if len(data_list) > 1:
          jresp['slice_list'] = ",".join(data_id for data_id in data_list[:-1])
        return jresp
    except ValueError as err:
      logging.debug("Can' parse data, please wait.")
      return None

class MulticastBroker:
  def __init__(self, multicast_address):
    host_ip = get_host_ip()
    self.multicast_host, self.multicast_port = multicast_address
    self.multicast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    self.multicast_socket.bind((host_ip, 0))
    self.data_handler = self.default_data_handler
    self.interfaces = [host_ip,]
    self.stop = False
    logging.debug(self.interfaces)

  def default_data_handler(self, data):
    pass
  
  def set_data_handler(self, handler):
    self.data_handler = handler

  def stop_receive(self):
    self.stop = True

  def receive_loop(self):
    self.stop = False
    receive_sockets = []
    for interface in self.interfaces:
      sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
      sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      sock.bind(("0.0.0.0", self.multicast_port))
      mreq = struct.pack('4s4s', socket.inet_aton(self.multicast_host), socket.inet_aton(interface))
      sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
      receive_sockets.append(sock) 

    loop = True
    while loop and not self.stop:
      (sread, swrite, sexc) = select.select(receive_sockets, [], [], 0.1)
      for sock in sread:
        try:
          data = sock.recv(MAX_UDP_PACKET_SIZE)
#          logging.debug("Multicast recv %d bytes", len(data))
          if len(data):
            if(self.data_handler(data) == False):
              logging.debug("Exit recve_loop")
              loop = False
              break;
        except OSError as err:
          logging.error(err)
          loop = False

    for sock in receive_sockets:
      sock.close()

  
  def send(self, data):
    # for test, random throw packet
    #if random.randint(0,99) < 20:
    #  return

    if len(data) > MAX_UDP_PACKET_SIZE:
      logging.warning("Data send by multicast should less than %d, now is %d" %(MAX_UDP_PACKET_SIZE, len(data)))
#    logging.debug("Multicast send %d bytes", len(data))
    self.multicast_socket.sendto(data, (self.multicast_host, self.multicast_port))

class FileBlock:
  def __init__(self, name):
    self.file_name = name
  
  def block_iterator(self, block_size):
    self.block_size = block_size
    with open(self.file_name, 'rb') as file:
      while True:
        data = file.read(self.block_size)
        logging.debug("Block iterator require %d, actuall get %d", block_size, len(data))
        if len(data):
          yield data
        else:
          return

class MFileTransfer:
  def __init__(self, broker):
    self.broker = broker
    self.slice_buffer = {}
    self.current_block_index = 0

  # slice data include a  4 bytes header, use to store index of slice.
  def pack_slice(self, slice_data, block_index, slice_index):
    slice_header = struct.pack('2H', block_index, slice_index)
    return slice_header + slice_data
  
  def unpack_slice(self, data):
    block_index, slice_index = struct.unpack('2H', data[:4])
    return block_index,slice_index,data[4:]

  def send_block(self, block, slice_size):
    slice_count = int((len(block)-1)/slice_size) + 1
    logging.debug("Block size %d, slice count %d", len(block), slice_count)
    for block_slice_index in range(slice_count):
      file_slice = block[block_slice_index*slice_size:(block_slice_index+1)*slice_size]
      slice = self.pack_slice(file_slice, self.current_block_index, block_slice_index)
      self.broker.send(slice)
    return slice_count

  def file_transfer(self, server, file_path):
    self.current_block_index = 0
    if not os.path.exists(file_path):
      logging.error("file %s not exist!", file_path)
      return

    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    max_block_size = DEFAULT_BLOCK_SIZE
    max_slice_size = DEFAULT_SLICE_SIZE
    # notify all client to receive file.
    server.transfer_start_request(file_path, file_size, max_block_size, max_slice_size)
    file_block = FileBlock(file_path)
    block_sum = 0
    total_retransmission_count = 0
    total_slice_count = 0

    # we sleep here, wait for client to become ready.
    time.sleep(0.3)
    start_time = time.time()
    logging.info("Start deploy %s", file_name)
    for block in file_block.block_iterator(max_block_size):
      # send block to client
      total_slice_count += self.send_block(block, max_slice_size)
      # last block
      is_last_block = False
      if file_size - block_sum <= max_block_size:
        is_last_block = True
      # retransmit all losing slice
      while True:
#        time.sleep(0.5)
        server.need_block_complete_confirm(True)
        retransmission_slices = server.get_retransmission_slices(self.current_block_index, is_last_block)
        total_retransmission_count += len(retransmission_slices)
        if not len(retransmission_slices):
          break;
        self.retransmit(retransmission_slices, block, max_slice_size)
      block_sum += len(block)
      self.current_block_index += 1
      logging.info("Transmit %d", (block_sum/file_size)*100)

    logging.info("Transfer file %s success, slice %d, retransmiss %d, spend %f"%(
                 file_name, total_slice_count, total_retransmission_count, time.time()-start_time))

  def retransmit(self, slices, block, slice_size):
    for slice_index in slices:
      slice_start = slice_size*slice_index
      slice_end = slice_start+slice_size
      slice_end = slice_end if slice_end <= len(block) else slice_start+(len(block)-slice_start)
      slice = self.pack_slice(block[slice_start:slice_end], self.current_block_index, slice_index)
      self.broker.send(slice)

  def save_file(self, client, path_to_save, file_info):
    self.slice_buffer = {}
    self.current_block_index = 0
    file_name,file_size,block_size,slice_size = file_info
    file_name = os.path.basename(file_name)
    slice_count = int((block_size-1) / slice_size) + 1
    if not os.path.exists(path_to_save) or not os.path.isdir(path_to_save):
      logging.error("path \"%s\" not existe or not directory, saving to current directory %s."%(path_to_save,file_name))
      path_to_save = file_name
    else:
      path_to_save = path_to_save + '/' + file_name
      logging.info("Saving file to %s"%(path_to_save))
    # receive block complete confirm request
    with open(path_to_save, 'wb') as saved_file:
      while True:
        not_finish, server_block_index = client.wait_server_command()
        if True == not_finish:
          if server_block_index < self.current_block_index:
            missing_slice = []
          else:
            missing_slice = self.get_missing_slice(slice_count)
          if not len(missing_slice):
            self.sync_data(saved_file, slice_count)
          client.retransmission_request(missing_slice)
        else: #receive file transfer complete confirm request
          left_file_size = file_size - self.current_block_index*block_size
          logging.debug("left file size: %d", left_file_size)
          slice_count = int((left_file_size-1)/slice_size + 1)
          missing_slice = self.get_missing_slice(slice_count) 
          client.retransmission_request(missing_slice)
          if not len(missing_slice):
            self.sync_data(saved_file, slice_count)
            break
      logging.info("Saving file %s success!",file_name)

  def get_missing_slice(self, slice_count):
    missing_slices = []
    for slice_index in range(slice_count):
      if slice_index not in self.slice_buffer:
        missing_slices.append(slice_index)
    logging.error("Ask %d retransmission slice"%len(missing_slices))
    return missing_slices
  
  def sync_data(self, file, slice_count):
    logging.debug("curren_block_index: %d, slice_count: %d"%(self.current_block_index, slice_count))
    for slice_index in range(slice_count):
      file.write(self.slice_buffer[slice_index])
    self.slice_buffer = {}
    self.current_block_index += 1

  def receive_slice_handler(self, data):
    
    block_index,slice_index,buffer = self.unpack_slice(data)
    if block_index >= self.current_block_index and (slice_index not in self.slice_buffer):
#      logging.debug("Receive slice %d" % slice_index)
      self.slice_buffer[slice_index] = buffer


def input_handler(input, server, transfer):
  file_path = input
  return transfer.file_transfer(server, file_path)

def main(args):
  LOG_FORMAT = "[%(asctime)s:%(levelname)s:%(funcName)s]  %(message)s"
  log_level = logging.INFO
  if args.debug:
    log_level = logging.DEBUG

  logging.basicConfig(level=log_level, format=LOG_FORMAT)

  if not args.multicast_address or args.multicast_address.find(':') == -1:
    multicast_ip = "239.0.0.100"
    multicast_port  = "5004"
    logging.info("Using default muticast address %s:%s", multicast_ip, multicast_port)
  else:
    multicast_ip,multicast_port = args.multicast_address.split(":")

  if not args.address:
    logging.critical("Need server address as input, like \"127.0.0.1:60001\"")
    arg_parser.print_help()
    exit()

  server_ip, server_port = args.address.split(':')

  sender_broker = MulticastBroker((multicast_ip, int(multicast_port)))
  transfer = MFileTransfer(sender_broker)

  if args.client:
    command_client = MFileTransferClient(CommandHandler())
    if command_client.connect(server_ip, int(server_port)):
      while True:
        file_info = command_client.wait_transfer_start()
        logging.debug("Get start signal")
        sender_broker.set_data_handler(transfer.receive_slice_handler)
        receive_thread = threading.Thread(target=sender_broker.receive_loop)
        receive_thread.daemon = True
        receive_thread.start()
  
        path_to_save = args.path_to_save if args.path_to_save else "/tmp"
        transfer.save_file(command_client, path_to_save, file_info)
        sender_broker.stop_receive()
        receive_thread.join()
  elif args.server:
    server = MFileTransferServer((server_ip, int(server_port)), CommandHandler())
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

if __name__ == "__main__":

  arg_parser = argparse.ArgumentParser(description="manual to this script")
  arg_parser.add_argument('-c', "--client", help="run in client mode",
                          action="store_true")
  arg_parser.add_argument('-s', "--server", help="run in server mode",
                          action="store_true")
  arg_parser.add_argument('-d', "--debug", help="enable debug mode",
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

  if not args.client and not args.server:
      logging.warning("Run in qpython as client mode")
      args.client=True
      args.address="172.18.93.85:60000"
      args.debug=True
  main(args)


