import socket  
  
address = ('10.211.55.10', 514)  
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  
  
while True:  
    msg = raw_input()  
    if not msg:  
        break  
    s.sendto(msg, address)  
  
s.close()  
