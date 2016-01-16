import sys, zmq, threading

lock = threading.Lock()

def work(tid, ctx):
    sock = ctx.socket(zmq.REQ)
    sock.connect("ipc:///var/tmp/echo-outer.sock")
    sock.send('%s %d' % (' '.join(sys.argv[1:]), tid))
    recvd = sock.recv()
    sock.close()
    with lock:
        print("Thread %d received %s" % (tid, recvd))
    
ctx = zmq.Context()

threads = []
for i in range(10):
    thread = threading.Thread(None, work, args = (i, ctx))
    threads.append(thread)
    thread.start()

for i, thread in enumerate(threads):
    thread.join()

    
