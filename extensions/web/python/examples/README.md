# Python Web examples

These examples cover the Web extension's graph-owned transports:

- [`hello_server.py`](hello_server.py) serves a small HTTP endpoint and keeps
  server registration in the outer host graph.
- [`http_client.py`](http_client.py) performs one HTTP request and handles the
  response and transport-failure arms separately.
- [`websocket_loopback.py`](websocket_loopback.py) runs an ephemeral WebSocket
  server and client in one graph, sends a frame, echoes it, and stops.

Run the server and request it from another terminal:

```sh
python extensions/web/python/examples/hello_server.py --port 8080
curl http://127.0.0.1:8080/hello
```

The loopback example is self-contained:

```sh
python extensions/web/python/examples/websocket_loopback.py
```

Registration declares lazy, path-bound services. Sockets and worker threads
are created when the graph starts and are stopped with it; there is no server
or client object for application code to poll or close.
