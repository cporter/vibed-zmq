import random, sys, time, zmq

ctx = zmq.Context()
sock = ctx.socket(zmq.REP)
sock.bind("ipc:///var/tmp/echo-inner.sock")

while True:
    recvd = sock.recv_multipart()
    print('Reveived %s' % recvd)
    # time.sleep(random.random() * 0.2)
    sock.send_multipart(recvd)
    print('Sent %s' % recvd)
    
