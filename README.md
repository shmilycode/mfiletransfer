
# Usage
### options
1. "-c": Run as client
2. "-s": Run as server
3. "-d": Debug mode, output more debug information
4. "-a": Server address for client, if run as server ,also need to use this option, and set to current network interface.
5. "-F": Path to save file.
6. "-m": multicast address, default is 225.100.100.6:5555

### examples:
1. server
2. client

# 程序时序图(Use PlantUML)

@startuml
participant Server
participant Sender 
participant Client
Server -> Client: <<TransferStartNotify>>
Client --> Server: <<TransferStartResponse>>
loop until all blocks sent
  Server -> Sender: File block
  Sender -> Client: Send
  loop until all slices sent
    Server -> Client: <<BlockCompleteConfirm>>
    activate Server
      activate Client
          Client -> Client: Count lossing slice
          Client --> Server: <<RetransmissionRequest>>
      deactivate Client
      activate Server
        Server -> Server: Clean dumplicate packet
        Server -> Sender: Retransmission
      deactivate Server
      Sender -> Client: Send
    deactivate Server
  end
end

@enduml