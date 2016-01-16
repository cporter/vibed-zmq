import vibe.core.core : runTask;
import vibe.core.task : Task;
import core.time : msecs;

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

class PrependSock(Sock)
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

template hasContext(T)
{
    import yazl : Context;

    enum hasContext = is(typeof(T.init.context) : Context);
}

template hasQuit(T)
{
    enum hasQuit = is(typeof(T.init.quit) == bool);
}

static Task[] child_workers;
void idleHandler()
{
    import std.algorithm : filter;
    import std.array : array;

    child_workers = child_workers.filter!(x => x.running).array;
}

static void routerHandler(alias workerFun, Universe)(ref Universe uni) if (
        hasContext!Universe && hasQuit!Universe)
{
    import std.algorithm : filter;
    import yazl : Context, Socket, SocketType;
    import std.typecons : scoped;
    import vibe.core.task : InterruptException;
    import std.stdio : writeln;
    import vibe.core.core : exitEventLoop;

    auto router_sock = new Socket(uni.context, SocketType.router);
    router_sock.linger = 100.msecs;
    scope (exit)
    {
        router_sock.close();
    }

    router_sock.bind("ipc:///var/tmp/echo-outer.sock");

    try
    {
        while (!uni.quit)
        {
            if (router_sock.waitForInput(100.msecs))
            {
                auto sock = new PrependSock!Socket(router_sock);
                sock.linger = 100.msecs;
                // workerFun(uni, sock);
                child_workers ~= runTask({ workerFun(uni, sock); });
            }
        }
        exitEventLoop();
    }
    catch (InterruptException ie)
    {
    }
    foreach (i, ref child; child_workers)
    {
        child.terminate();
        child.join();
    }
}

// An example handler function. Valid handlers are any function that
// take a $(D ref Universe) and a $(D SockType) as arguments. They're
// meant to handle one request. They must always read all data from
// their socket before performing any operation that might yeild. Assume
// that the function is always called immediately following a socket's
// waitForInput returning successfully.
//
// The $(D Universe) type is templated such that it could contain whatever
// the application needs. Things like a database connection pool, the
// ZMQ context in use by the calling socket, etc.
//
// In this particular implementation, the argument is always a
// $(D PrependSock), which will read the socket's header for you and make
// sure that its sent back with the request. So you don't have to
// worry about that step of zmq management on your own. The thinking
// here is that, for the handler function is the same for either REP or
// ROUTER sockets. 
void simpleReqForwarder(Universe, SockType)(ref Universe uni, SockType sock) if (
        hasContext!Universe)
{
    import yazl : Socket, SocketType;

    // You need to read all of the payload before you do anything
    // that might yield execution. 
    auto payload = sock.recvMultipart();

    // here's what we're forwarding to. Maybe the endpoint would
    // live in the Universe, or maybe there'd be a connection pool
    // there. If you create a socket in a worker, make double sure
    // that it gets cleaned up when you exit scope.
    auto fwd = new Socket(uni.context, SocketType.req);
    scope (exit)
    {
        fwd.close();
    }
    fwd.connect("ipc:///var/tmp/echo-inner.sock");

    fwd.sendMultipart(payload);

    // Busy-waiting to allow the application to quit mid-recv.
    bool has_input = false;
    while (!(has_input || uni.quit))
    {
        has_input = fwd.waitForInput(100.msecs);
    }
    if (!uni.quit)
    {
        auto fwded = fwd.recvMultipart();
        sock.sendMultipart(fwded);
    }
}

struct AppUniverse
{
    import yazl : Context, SocketType;

    Context context;
    bool quit = false;

    ~this()
    {
        if (context !is null)
        {
            context.close();
        }
    }

    static AppUniverse make()
    {
        AppUniverse uni;
        uni.context = new Context();
        return uni;
    }
}

static AppUniverse static_universe;
extern (C) private void handleSigterm(int signal)
{
    if (11 == signal || 2 == signal)
    {
        static_universe.quit = true;
    }
}

shared static this()
{
    import vibe.d : setIdleHandler;
    import std.functional : toDelegate;
    import core.sys.posix.signal : bsd_signal, SIGTERM, SIGINT;

    static_universe = AppUniverse.make();
    bsd_signal(SIGTERM, &handleSigterm);
    bsd_signal(SIGINT, &handleSigterm);
    setIdleHandler(toDelegate(&idleHandler));
    runTask({ routerHandler!simpleReqForwarder(static_universe); });
}
