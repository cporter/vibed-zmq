module yazl.router;

import core.time : msecs;
import vibe.core.core : runTask;
import vibe.core.task : Task;
import yazl.sock : Socket;

private:

ubyte[][] extractHeader(Sock)(ref Sock sock) if (is(ubyte[] == typeof(Sock.recv())))
{
    ubyte[][] ret;
    ubyte[] buf;
    do
    {
        buf = sock.recv();
        ret ~= buf;
    }
    while (0 < buf.length);
    return ret;
}

struct PrependSock(Sock)
{
    Sock sock;
    ubyte[][] header;
    bool header_sent = false;

    this(Sock _sock)
    {
        sock = _sock;
        header = sock.extractHeader();
    }

    private void sendHeaders()
    {
        if (!header_sent)
        {
            foreach (buf; header)
            {
                sock.send(buf, true);
            }
            header_sent = true;
        }
    }

    int send(ubyte[] data, bool more = false)
    {
        sendHeaders();
        return sock.send(data, more);
    }

    void sendMultipart(ubyte[][] bufs)
    {
        sendHeaders();
        sock.sendMultipart(bufs);
    }

    alias sock this;
}

static Task[] child_workers;

public:

void idleHandler()
{
    import std.algorithm : filter;
    import std.array : array;

    child_workers = child_workers.filter!(x => x.running).array;
}

static void routerHandler(alias workerFun, Universe)(auto ref Universe uni, Socket router_sock)
{
    import vibe.core.task : InterruptException;

    try
    {
        for (;;)
        {
            if (router_sock.waitForInput(100.msecs))
            {
                child_workers ~= runTask({
                    auto sock = PrependSock!Socket(router_sock);
                    workerFun(uni, sock);
                });
            }
            static if (__traits(hasMember, Universe, "quit"))
            {
                if (uni.quit)
                {
                    break;
                }
            }
        }
    }
    catch (InterruptException ie)
    {
    }
    foreach (ref child; child_workers)
    {
        child.terminate();
        child.join();
    }
}
