# ZMQ/vibe.d sketch

Beginnings of a framework to use ZeroMQ and Vibe.d in a plesant way.

## Goals

Provide a framework such that you can, without too much fuss or ceremony,
write a "handler" function to service a ZMQ socket. The current
implementation focuses on router sockets, but allows for task-based
concurrency when reading from all types of socket. It does not handle
write-blockable sockets at all, instead assuming that all socket types
are always available for writing.

## Running the example

The [current example program](source/app.d) acts as a simple forwarder from
a ROUTER socket to a REP socket. There are python scripts to act both as the
client and the back-end server. To run the backend:

    python pong.py

To run the example application, run `dub` per usual. Once both are running,
you can run the client:

    python ping.py hello world

The client will spawn ten threads which will all send a REQ in to the
vibed-zmq process, which will pass them on to `pong` and return the results
to the correct client.
