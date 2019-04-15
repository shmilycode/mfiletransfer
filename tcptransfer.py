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
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED, as_completed

MAX_WORKERS = 60
MAX_TCP_PACKET_SIZE = 4096

def start_transfer(client, data):
  logging.debug("Start sending to %s:%d"%client.getpeername())
  client.sendall(data)
  logging.debug("Sending to %s:%d finished"%client.getpeername())
  return client


class MFileTransferServer:
  def __init__(self, address, command_handler):
    self.addres = address 
    self.command_handler = command_handler
    self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.server_socket.bind(address)
    self.server_socket.listen(socket.SOMAXCONN)
    self.descriptors = [self.server_socket,]
    self.client_buffer = {}
    self.task_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    self.response_client_list = []
  
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
              self.request_handler(client, data)
            else:
              logging.info("%s:%s client disconnected!" % client.getpeername())
              client.close()
              self.descriptors.remove(client)
          except OSError as err:
            logging.error(err)
            client.close()
            self.descriptors.remove(client)
            del self.client_buffer[client]

  def request_handler(self, client, data):
    request = self.command_handler.parse_client_request(data)
    # failed to decode, fill to buffer
    if not request:
      return

    logging.debug(request)
    rtype = request['type']
    if rtype == CommandHandler.ReadyToReceive or (
      rtype == CommandHandler.ClientFinish):
      self.response_client_list.append(client)
    if rtype == CommandHandler.ReadyToReceive and (
      len(self.response_client_list) == (len(self.descriptors) - 1)):
      self.start_all_transfer_task();
    if rtype == CommandHandler.ClientFinish:
      time = request['time']
      self.total_time += time
      if (len(self.response_client_list) == (len(self.descriptors) - 1)):
        logging.info("Mean value of tranfer time: %f", self.total_time/len(self.response_client_list))


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

  def transfer_start_request(self, name, size, data):
    self.transfer_start_time = time.time()
    self.file_data = data
    request = self.command_handler.build_transfer_start_request(
      name, size)
    self.broadcast(request)
    self.response_client_list = []
    logging.info("Total client: %d", len(self.descriptors)-1)
  
  def start_all_transfer_task(self):
    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    all_task = []
    for client in self.descriptors[1:]:
      all_task.append(executor.submit(start_transfer, client, self.file_data))
    logging.debug(len(all_task))
    for future in as_completed(all_task):
      client = future.result()

    self.transfer_end_time = time.time()
    logging.info("Transfer all finished, spend %f!"%(self.transfer_end_time-self.transfer_start_time))
    self.response_client_list = []
    self.total_time = 0.0

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
        if result["type"] != CommandHandler.TransferStartNotify:
          continue
        return result["name"],result["size"]
      else:
        break;
  
  def ready_to_receive(self):
    request = self.command_handler.build_ready_to_receive_request()
    self.socket.sendall(request)

  def send_client_finish(self, time):
    request = self.command_handler.build_client_finished_confirm(time)
    self.socket.sendall(request)

  def get_file(self, file_name, file_size):
    size_read = 0
    time_count = 0
    with open(file_name, 'wb') as file_to_write:
      while size_read < file_size:
        data = self.socket.recv(MAX_TCP_PACKET_SIZE)
        data_size = len(data)
#        logging.debug("Receive %d"%data_size)
        if data_size:
          size_read += len(data)
          file_to_write.write(data)
        else:
          break;
        if not (time_count % 20):
          sys.stdout.write("%.2f"%((size_read/file_size)*100))
          sys.stdout.write('\r') 
          sys.stdout.flush()
          time_count = 0
        else:
          time_count += 1

# Singletone class
class CommandHandler(object):
  _instance_lock = threading.Lock()
  TransferStartNotify = 0
  ReadyToReceive = 1
  ClientFinish = 2

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
      ttype,name_size,name,size = struct.unpack('BB'+str(name_len)+'sQ', data)
      jresp = {"type": ttype, "name": name.decode('utf-8'), "size": size}
    else:
      logging.error('Unkonw type %d'%(request_type))
    logging.debug(jresp)
    return jresp

  def build_transfer_start_request(self, name, size):
    jresp = {"type": CommandHandler.TransferStartNotify, "name": name, "size": size}
  
    bresp = struct.pack('BB'+str(len(name))+'sQ', 
                        CommandHandler.TransferStartNotify, len(name), name.encode('utf-8'), size)
    logging.debug(jresp)
    return bresp

  def build_ready_to_receive_request(self):
    jresp = {"type": CommandHandler.ReadyToReceive}
  
    bresp = struct.pack('B',  CommandHandler.ReadyToReceive)
    logging.debug(jresp)
    return bresp

  def build_client_finished_confirm(self, time):
    jresp = {"type": CommandHandler.ClientFinish}
  
    bresp = struct.pack('Bf',  CommandHandler.ClientFinish, time)
    logging.debug(jresp)
    return bresp

  def build_block_complete_confirm(self):
    jresp = {"type": CommandHandler.BlockCompleteConfirm}
    bresp = struct.pack('B', CommandHandler.BlockCompleteConfirm)
    logging.debug(jresp)
    return bresp

  def build_transfer_complete_confirm(self):
    jresp = {"type": CommandHandler.TransferCompleteConfirm}
    bresp = struct.pack('B', CommandHandler.TransferCompleteConfirm)
    logging.debug(jresp)
    return bresp

  def parse_client_request(self, data):
    logging.debug("client request %d bytes"%len(data))
    rtype = ord(data[:1])
    if rtype == CommandHandler.ReadyToReceive:
      jresp = {'type': rtype}
    elif rtype == CommandHandler.ClientFinish:
      rtype,time = struct.unpack('Bf', data)
      jresp = {'type': rtype, 'time': time}
    return jresp


def file_transfer(server, file_path):
  if not os.path.exists(file_path):
    logging.error("file %s not exist!", file_path)
    return

  file_size = os.path.getsize(file_path)
  file_name = os.path.basename(file_path)
  # notify all client to receive file.
  with open(file_path, 'rb') as my_file:
    my_data = my_file.read()
    server.transfer_start_request(file_path, file_size, my_data)

def input_handler(input, server):
  file_path = input
  return file_transfer(server, file_path)


def main(args):
  LOG_FORMAT = "[%(asctime)s:%(levelname)s:%(funcName)s]  %(message)s"
  log_level = logging.INFO
  if args.debug:
    log_level = logging.DEBUG

  logging.basicConfig(level=log_level, format=LOG_FORMAT)

  if not args.address:
    logging.critical("Need server address as input, like \"127.0.0.1:60001\"")
    arg_parser.print_help()
    exit()

  server_ip, server_port = args.address.split(':')

  if args.client:
    command_client = MFileTransferClient(CommandHandler())
    if command_client.connect(server_ip, int(server_port)):
      while True:
        file_name,file_size = command_client.wait_transfer_start()
        logging.debug("Get start signal")
        start_time = time.time()
        command_client.ready_to_receive()
        command_client.get_file(file_name, file_size)
        end_time = time.time()
        logging.error("Save file success, spend %f", end_time-start_time)
        command_client.send_client_finish(end_time-start_time)

  elif args.server:
    server = MFileTransferServer((server_ip, int(server_port)), CommandHandler())
    server_ip, server_port = server.server_address()

    server_thread = threading.Thread(target=server.run)
    server_thread.daemon = True
    server_thread.start()
    logging.info("Server %s:%d loop running in thread: %s"%(server_ip, server_port, server_thread.name))

    for line in sys.stdin:
      is_exit = input_handler(line.strip('\n'), server)
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
  arg_parser.add_argument('-f', "--name", help="file path", 
                          type=str)
  arg_parser.add_argument('-F', "--path_to_save", help="path to save file which was recevied from server ", 
                          type=str)
  args = arg_parser.parse_args()

  if not args.client and not args.server:
      logging.error("Run in qpython as client mode")
      args.client=True
      args.address="172.18.93.85:60000"
      args.debug=True
  main(args)
