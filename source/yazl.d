
/// yazl: Yet Another ØMQ Library
///
/// Inspired by:
/// - https://github.com/kyllingstad/zmqd (for @safe-safety)
/// - https://github.com/iccodegr/zmq.d (for vibe.d concurrency)
///
/// Requires vibe.d. Only particularly useful *within* the convines
/// of vibe.d. 

module yazl;


import deimos.zmq.zmq;
import core.time : Duration;
import core.stdc.errno : errno, EINTR;
import std.exception : ErrnoException;

version(Have_vibe_d) {
    import vibe.core.core : createFileDescriptorEvent, FileDescriptorEvent, TaskMutex;
} else {
    static assert(false, "Vibe.d is required.");
}

class ZMQException : ErrnoException {
    this(string msg) @safe { super(msg); }
}

class Context {
    @safe:
    
    private void *handle;
    private Socket[] child_sockets;

    private void registerSocket(Socket s) nothrow {
        child_sockets ~= s;
    }

    private void unregisterSocket(Socket s) nothrow {
        import std.algorithm : remove;
        child_sockets.remove!(sock => sock is s);
    }
    
    this() {
        handle = trusted!zmq_ctx_new();
        assert(handle !is null);
    }

    ~this() {
        int rc = trusted!zmq_ctx_destroy(handle);
        assert(-1 != rc, "Context was not in a closeable state.");
    }

    void close() {
        int rc;
        foreach (sock; child_sockets) {
            sock.close();
        }
        
        do {
            rc = trusted!zmq_ctx_destroy(handle);
        } while(-1 == rc && EINTR == errno);
        if (-1 == rc) {
            throw new ZMQException("Could not close context.");
        }
    }
    
    @property int threads() {
        assert(handle !is null);
        int rc = trusted!zmq_ctx_get(handle, ZMQ_IO_THREADS);
        assert(-1 != rc);
        return rc;
    }

    @property int maxSockets() {
        assert(handle !is null);
        int rc = trusted!zmq_ctx_get(handle, ZMQ_MAX_SOCKETS);
        assert(-1 != rc);
        return rc;
    }

    @property void threads(int t) {
        assert(handle !is null);        
        auto rc = trusted!zmq_ctx_set(handle, ZMQ_IO_THREADS, t);
        assert(-1 != rc, "Invalid number of threads specified");
    }

    @property void maxSockets(int m) {
        assert(handle !is null);
        auto rc = trusted!zmq_ctx_set(handle, ZMQ_MAX_SOCKETS, m);
        assert(-1 != rc, "Invalid max sockets specified");
    }
}

enum SocketType {
    req = ZMQ_REQ,
    rep = ZMQ_REP,
    dealer = ZMQ_DEALER,
    router = ZMQ_ROUTER,
}

class Socket {
    @safe:

    private {
        void* handle;
        FileDescriptorEvent read_evt;
        TaskMutex read_mutex;
        Context my_context;
    }

    ~this() nothrow {
        if (handle !is null) {
            auto rc = trusted!zmq_close(handle);
            my_context.unregisterSocket(this);
            assert(0 == rc, "Trouble closing a socket.");
            handle = null;
        }
    }

    void close() {
        if (handle !is null) {
            auto rc = trusted!zmq_close(handle);
            my_context.unregisterSocket(this);
            handle = null;
            if(0 != rc) {
                throw new ZMQException("Could not close handle.");
            }
        }
    }

    private T getSockopt(T)(int sockopt) @trusted {
        assert(handle !is null, "getSockopt called with a null socket.");
        T tmp;
        size_t len = tmp.sizeof;
        int rc;
        rc = trusted!zmq_getsockopt(handle, sockopt,
                                    cast(void*) &tmp,
                                    &len);
        if (0 != rc) {
            // We're only using hard-coded values with this API.
            // Sending an invalid sockopt is a logic error.
            import core.stdc.errno : EINVAL;
            assert(EINVAL != errno, "Invalid Sockopt.");
            
            import std.format : format;
            throw new ZMQException(format("getSockopt failed for sockopt %d", sockopt));
        }
        return tmp;
    }

    private void setSockopt(T)(int sockopt, in T value) @trusted {
        assert(handle !is null);
        auto rc = trusted!zmq_setsockopt(handle, sockopt, cast(void*)&value, T.sizeof);
        if (0 != rc) {
            // We're only using hard-coded values with this API.
            // Sending an invalid sockopt is a logic error.
            import core.stdc.errno : EINVAL;
            assert(EINVAL != errno, "Invalid Sockopt.");
            
            import std.format : format;
            throw new ZMQException(format("getSockopt failed for sockopt %d", sockopt));
        }
    }

    void bind(string endpoint) {
        assert(handle !is null);
        auto rc = trusted!zmq_bind(handle, endpoint.zeroTermString);
        if (-1 == rc) {
            throw new ZMQException("Could not bind to " ~ endpoint);
        }
    }

    void unbind(string endpoint) {
        assert(handle !is null);
        auto rc = trusted!zmq_unbind(handle, endpoint.zeroTermString);
        if (-1 == rc) {
            throw new ZMQException("Could not unbind from " ~ endpoint);
        }
    }

    void connect(string endpoint) {
        assert(handle !is null);
        auto rc = trusted!zmq_connect(handle, endpoint.zeroTermString);
        if (-1 == rc) {
            throw new ZMQException("Could not connect to " ~ endpoint);
        }
    }

    /// Disconnect from an endpoint
    /// Returns: true if successfully disconnected, false if not connected in the first place.
    bool disconnect(string endpoint) {
        auto rc = trusted!zmq_disconnect(handle, endpoint.zeroTermString);
        if (-1 == rc) {
            import core.stdc.errno : ENOENT;
            if (errno == ENOENT) {
                return false;
            }
            throw new ZMQException("Could not disconnect from " ~ endpoint);
        }
        return true;
    }
    
    @property int events() {
        return getSockopt!int(ZMQ_EVENTS);
    }

    @property bool canRead() {
        return 0 != (events & ZMQ_POLLIN);
    }

    @property bool canWrite() {
        return 0 != (events & ZMQ_POLLOUT);
    }

    @property bool more() {
        return 0 != getSockopt!long(ZMQ_RCVMORE);
    }

    @property int fd() {
        return getSockopt!int(ZMQ_FD);
    }

    @property void linger(Duration dur) {
        int ms = cast(int) dur.total!"msecs";
        setSockopt(ZMQ_LINGER, ms);
    }
    
    import core.time : Duration, msecs;

    /// Yield until the is data available to be read
    /// Returns:
    ///    bool true if data is available, false if we've timed out.
    /// Params:
    ///    timeout = A duration to wait before timing out.
    bool waitForInput(Duration timeout) @trusted {
        // if we're already ready, go for it.
        if (canRead) {
            return true;
        }
        
        if (read_evt.wait(timeout, FileDescriptorEvent.Trigger.read)) {
            return canRead;
        }
        return false;
    }

    /// Read a frame in to $(D buf). You must call $(D waitForInput) first.
    /// Returns: the number of bytes received
    /// Params:
    ///   buf = A buffer to store bytes in. Up to $(D buf.length) bytes will be read.
    int recv(ubyte[] buf) {
        assert(canRead, "Socket does not have data to read.");
        int rc = 0;
        do {
            rc = trusted!zmq_recv(handle, cast(void*)buf.ptr, buf.length, ZMQ_DONTWAIT);
        } while (-1 == rc && EINTR == errno);
        if (-1 == rc) {
            import core.stdc.errno : EAGAIN;
            assert(EAGAIN != errno, "recv called without waitForInput (presumably).");
            throw new ZMQException("Could not receive.");
        }
        return rc;
    }

    /// Read a frame. You must call $(D waitForInput) first.
    /// Returns: a buffer bytes read from the current frame.
    ubyte[] recv() @trusted {
        assert(canRead, "Socket does not have data to read.");
        zmq_msg_t msg;
        int rc = trusted!zmq_msg_init(&msg);
        assert(-1 != rc, "Could not create zmq_msg_t");
        scope(exit) {
            rc = trusted!zmq_msg_close(&msg);
            assert(-1 != rc, "Could not close zmq_msg_t");
        }
        do {
            rc = trusted!zmq_msg_recv(&msg, handle, ZMQ_DONTWAIT);
        } while (-1 == rc && EINTR == errno);

        if (-1 == rc) {
            import core.stdc.errno : EAGAIN;
            assert(EAGAIN != errno, "recv called without waitForInput (presumably).");
            throw new ZMQException("Could not receive.");
        }
        
        ubyte[] buf;
        buf.length = trusted!zmq_msg_size(&msg);
        buf[] = (cast(ubyte*) trusted!zmq_msg_data(&msg))[0..buf.length];
        return buf;
    }

    /// Send a buffer in a zmq frame
    /// Returns: the number of bytes sent
    /// Params:
    ///    buf = The buffer to send. $(D buf.length) bytes will be sent
    ///    send_more = $(D true) if you want to set the $(D ZMQ_SNDMORE) flag.
    int send(ubyte[] buf, bool send_more = false) {
        assert(canWrite);
        int flags = send_more ? ZMQ_SNDMORE : 0;
        int rc;
        do {
            rc = trusted!zmq_send(handle, buf.ptr, buf.length, flags);
        } while (-1 == rc && EINTR == errno);
        if (-1 == rc) {
            throw new ZMQException("Could not send.");
        }
        return rc;
    }

    /// A helper function to read all of a multi-frame message. Think Python.
    ubyte[][] recvMultipart() {
        ubyte[][] ret;
        do {
            ret ~= recv();
        } while (more);
        return ret;
    }

    /// A helper functino to send all of a multi-frame message. Think Python.
    void sendMultipart(ubyte[][] bufs) {
        immutable N = bufs.length - 1;
        foreach (i, buf; bufs) {
            auto sent = send(buf, i < N);
            assert(sent == buf.length);
        }
    }

    this() @disable;
    
    this(Context ctx, SocketType sock_type) @trusted {
        assert(ctx.handle !is null);
        handle = trusted!zmq_socket(ctx.handle, cast(int) sock_type);
        assert(handle !is null);
        read_evt = createFileDescriptorEvent(fd, FileDescriptorEvent.Trigger.read);
        read_mutex = new TaskMutex();
        my_context = ctx;
        my_context.registerSocket(this);
    }
}

// From https://github.com/kyllingstad/zmqd/blob/master/src/zmqd.d

private auto trusted(alias func, Args...)(auto ref Args args) @trusted
{
    return func(args);
}

private const(char)* zeroTermString(const char[] s) nothrow @safe
{
    import std.algorithm: max;
    static char[] buf;
    immutable len = s.length + 1;
    if (buf.length < len) buf.length = max(len, 1023);
    buf[0 .. s.length] = s;
    buf[s.length] = '\0';
    return buf.ptr;
}
