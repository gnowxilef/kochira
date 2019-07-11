"""
Omegle.

Talk to strangers!
"""

import json
import random

from pydle import Future, coroutine, parallel

from urllib.parse import urlencode

from kochira import config
from kochira.service import Service, Config, coroutine

from tornado.httpclient import AsyncHTTPClient, HTTPRequest, HTTPError

service = Service(__name__, __doc__)


@service.config
class Config(Config):
    hosts = config.Field(doc="List of hosts to connect to.",
                         default=["front1.omegle.com",
                                  "front2.omegle.com",
                                  "front3.omegle.com",
                                  "front4.omegle.com",
                                  "front5.omegle.com",
                                  "front6.omegle.com",
                                  "front7.omegle.com",
                                  "front8.omegle.com",
                                  "front9.omegle.com"])
    ignore_prefix = config.Field(doc="Ignore all messages starting with a prefix.",
                                 default=",")


class OmegleError(Exception):
    pass


class Connection:
    def __init__(self, host):
        self.host = host
        self.http_client = AsyncHTTPClient()
        self.id = None

    @coroutine
    def _raw_request(self, endpoint, method="POST", **params):
        if method == "GET":
            req = HTTPRequest(
                "http://" + self.host + "/" + endpoint + "?" + urlencode(params),
                method="GET"
            )
        elif method == "POST":
            req = HTTPRequest(
                "http://" + self.host + "/" + endpoint,
                method="POST",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded"
                },
                body=urlencode(params)
            )
        else:
            raise ValueError("unknown method")

        return (yield self.http_client.fetch(req)).body.decode("utf-8")

    def _request(self, endpoint, **params):
        if self.id is None:
            raise ValueError("not connected")

        return self._raw_request(endpoint, id=self.id, **params)

    @coroutine
    def connect(self):
        self.id = json.loads((yield self._raw_request("start", method="GET", rcs=1, spid="")))

    @coroutine
    def send(self, msg):
        if (yield self._request("send", msg=msg)) != "win":
            raise OmegleError

    @coroutine
    def recaptcha(self, challenge, response):
        if (yield self._request("recaptcha", challenge=challenge,
                                response=response)) != "win":
            raise OmegleError

    @coroutine
    def typing(self):
        if (yield self._request("typing")) != "win":
            raise OmegleError

    @coroutine
    def stopped_typing(self):
        if (yield self._request("stoppedTyping")) != "win":
            raise OmegleError

    @coroutine
    def disconnect(self):
        try:
            if (yield self._request("disconnect")) != "win":
                raise OmegleError
        finally:
            yield self.on_disconnect()
            self.id = None

    @coroutine
    def poll(self):
        while self.id is not None:
            try:
                yield self._poll_once()
            except HTTPError as e:
                if e.code == 599:
                    continue

                yield self.on_disconnect()
                self.id = None

    @coroutine
    def _poll_once(self):
        events = json.loads((yield self._request("events")))

        if events is None:
            return

        for event_type, *body in events:
            cb_name = "on_raw_" + event_type

            if hasattr(self, cb_name):
                yield getattr(self, cb_name)(*body)

    @coroutine
    def on_raw_strangerDisconnected(self):
        yield self.on_disconnect()
        self.id = None

    @coroutine
    def on_raw_gotMessage(self, body):
        yield self.on_message(body)

    @coroutine
    def on_raw_antinudeBanned(self, body):
        yield self.on_banned(body)

    @coroutine
    def on_raw_connected(self):
        yield self.on_connect()

    @coroutine
    def on_raw_recaptchaRequired(self, body):
        yield self.on_recaptcha(*body)

    @coroutine
    def on_connect(self):
        pass

    @coroutine
    def on_disconnect(self):
        pass

    @coroutine
    def on_message(self, message):
        pass

    @coroutine
    def on_recaptcha(self, challenge):
        pass


class IRCBoundConnection(Connection):
    def __init__(self, ctx, host):
        self.ctx = ctx
        super().__init__(host)

    @coroutine
    def on_message(self, message):
        self.ctx.message(self.ctx._("\x02[Omegle] {id}:\x02 {message}").format(
            id=self.id,
            message=message
        ))

        k = (self.ctx.client.name, self.ctx.target)
        futs = []

        for conn in self.ctx.storage.connections.get(k, set([])):
            if conn is self:
                continue

            futs.append(conn.send(message))

        yield parallel(*futs)

    @coroutine
    def on_recaptcha(self, challenge):
        k = (self.ctx.client.name, self.ctx.target)

        if k in self.ctx.storage.connections:
            self.ctx.storage.connections[k].remove(self)
            self.ctx.message(self.ctx._("\x02[Omegle] {id}\x02 asked for reCAPTCHA, but I don't want to process this.").format(
                id=self.id
            ))

            yield self.on_disconnect()
            self.id = None

    @coroutine
    def on_banned(self, message):
        k = (self.ctx.client.name, self.ctx.target)

        if k in self.ctx.storage.connections:
            self.ctx.storage.connections[k].remove(self)
            self.ctx.message(self.ctx._("\x02[Omegle] {id}\x02 responded with ban message \"{message}\". You're banned, doofus.").format(
                message=message,
                id=self.id
            ))

            yield self.on_disconnect()
            self.id = None

    @coroutine
    def on_disconnect(self):
        k = (self.ctx.client.name, self.ctx.target)

        if k in self.ctx.storage.connections and self in self.ctx.storage.connections[k]:
            self.ctx.storage.connections[k].remove(self)
            self.ctx.message(self.ctx._("\x02[Omegle] {id}\x02 disconnected.").format(
                id=self.id
            ))


@service.setup
def initialize_contexts(ctx):
    ctx.storage.connections = {}


@service.shutdown
def close_connections(ctx):
    @coroutine
    def _coro():
        futs = []

        for connections in ctx.storage.connections.values():
            for conn in connections:
                futs.append(conn.disconnect())

        yield parallel(*futs)

    fut = _coro()
    @fut.add_done_callback
    def _callback(future):
        exc = future.exception()
        if exc is not None:
            service.logger.error("Omegle unload error",
                                 exc_info=(exc.__class__, exc, exc.__traceback__))


@coroutine
def _make_connection(ctx):
    k = (ctx.client.name, ctx.target)

    host = random.choice(ctx.config.hosts)

    conn = IRCBoundConnection(ctx, host)
    yield conn.connect()
    ctx.respond(ctx._("Connected to \x02{id}\x02 via {host}.").format(
        id=conn.id,
        host=conn.host
    ))

    ctx.storage.connections.setdefault(k, set([])).add(conn)

    return conn


@service.command("!omegle connect")
@coroutine
def connect(ctx):
    """
    Connect to Omegle.

    Establish an Omegle connection.
    """

    conn = yield _make_connection(ctx)

    fut = conn.poll()

    @fut.add_done_callback
    def _callback(future):
        exc = future.exception()
        if exc is not None:
            service.logger.error("Omegle connection error",
                                 exc_info=(exc.__class__, exc, exc.__traceback__))


@service.command("!omegle disconnect")
@coroutine
def disconnect(ctx):
    """
    Disconnect from Omegle.

    Sever all Omegle connections.
    """
    k = (ctx.client.name, ctx.target)

    if k not in ctx.storage.connections:
        ctx.respond(ctx._("I'm not connected to any Omegle users."))
        return

    futs = []

    for client in set(ctx.storage.connections[k]):
        futs.append(client.disconnect())

    yield parallel(*futs)

    del ctx.storage.connections[k]


@service.command("!omegle cycle")
@coroutine
def cycle(ctx):
    """
    Cycle the chat.

    Closes the chat then reopens it.
    """
    yield disconnect(ctx)
    yield connect(ctx)


@service.command("!omegle names")
def names(ctx):
    """
    List connected clients.

    Shows all connected Omegle clients.
    """
    k = (ctx.client.name, ctx.target)

    ctx.respond(ctx._("The following users are connected: {users}").format(
        users=", ".join(c.id for c in ctx.storage.connections.get(k, set([])))
    ))


@service.hook("channel_message")
@coroutine
def relay_irc(ctx, target, origin, message):
    if message[0] == ctx.config.ignore_prefix:
        return

    k = (ctx.client.name, ctx.target)
    futs = []

    for conn in ctx.storage.connections.get(k, set([])):
        @coroutine
        def _coro():
            yield conn.typing()
            yield conn.send(message)

        futs.append(_coro())

    yield parallel(*futs)
