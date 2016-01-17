import core.time : msecs;
import yazl.sock : Socket, SocketType;

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
void simpleReqForwarder(Sock)(ref AppUniverse uni, ref Sock sock)
{
    import yazl : SocketType;

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
    // Note that every time $(D fwd.waitForInput) is called, another
    // task is given a shot to run.
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
    import yazl.sock : Context;

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
    import vibe.d : setIdleHandler, runTask;
    import std.functional : toDelegate;
    import core.sys.posix.signal : bsd_signal, SIGTERM, SIGINT;
    import yazl.router : routerHandler, idleHandler;

    static_universe = AppUniverse.make();
    bsd_signal(SIGTERM, &handleSigterm);
    bsd_signal(SIGINT, &handleSigterm);
    setIdleHandler(toDelegate(&idleHandler));
    runTask({
        auto router_sock = new Socket(static_universe.context, SocketType.router);
        router_sock.linger = 100.msecs;
        scope (exit)
        {
            router_sock.close();
        }

        router_sock.bind("ipc:///var/tmp/echo-outer.sock");

        routerHandler!simpleReqForwarder(static_universe, router_sock);
    });
}
