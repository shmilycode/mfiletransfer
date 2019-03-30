# 程序时序图

@startuml
participant Server
participant Sender 
participant Client
Server -> Client: Start
loop
  Server -> Sender: Queue file block packet 
  Sender -> Client: Send packet
  Client -> Server: Retransmission request
  Server -> Sender: Queue dumplicate packet
  Sender -> Client: Send packet
  Client -> Server: Request next file block
end
@enduml