import logging
from collections import deque
import textwrap

from pydle import Client as _Client
from pydle import Future, coroutine
from pydle.features.rfc1459.protocol import MESSAGE_LENGTH_LIMIT

from .service import Service, HookContext

logger = logging.getLogger(__name__)


class Client(_Client):
    RECONNECT_MAX_ATTEMPTS = None
    context_factory = HookContext

    def __init__(self, bot, name, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._reconnect_timeout = None
        self._fd = None

        self.backlogs = {}
        self.bot = bot

        self.name = name
        self.network = name

    @property
    def config(self):
        return self.bot.config.clients[self.name]

    @classmethod
    def from_config(cls, bot, name, config):
        client = cls(bot, name, config.nickname,
            username=config.username,
            realname=config.realname,
            tls_client_cert=config.tls.certificate_file,
            tls_client_cert_key=config.tls.certificate_keyfile,
            tls_client_cert_password=config.tls.certificate_password,
            sasl_identity=config.sasl.identity,
            sasl_username=config.sasl.username,
            sasl_password=config.sasl.password
        )

        client.connect(
            hostname=config.hostname,
            password=config.password,
            source_address=(config.source_address, 0),
            port=config.port,
            tls=config.tls.enabled,
            tls_verify=config.tls.verify
        )

        return client

    def connect(self, *args, reconnect=False, attempt=0, **kwargs):
        logger.info("Connecting: %s", self.name)

        try:
            super().connect(*args, reconnect=reconnect,
                            eventloop=self.bot.event_loop,
                            **kwargs)
        except (OSError, IOError) as e:
            self._reset_attributes()
            self.on_disconnect(False)

    def on_disconnect(self, expected):
        super().on_disconnect(expected)
        self._run_hooks("disconnect", None, None, [expected])

    def _send_message(self, message):
        self.bot.defer_from_thread(super()._send_message, message)

    def on_ctcp_version(self, by, what, contents):
        self.ctcp_reply(by, "VERSION", self.bot.config.core.version)

    def on_connect(self):
        logger.info("Connected to IRC: %s", self.name)
        super().on_connect()

        for name, channel in self.bot.config.clients[self.name].channels.items():
            self.join(name, password=channel.password)

        self._run_hooks("connect", None, None)

    def _autotruncate(self, command, target, message, suffix="..."):
        hostmask = self._format_user_mask(self.nickname)
        chunklen = MESSAGE_LENGTH_LIMIT - len("{hostmask} {command} {target} :".format(
            hostmask=hostmask,
            command=command,
            target=target
        )) - 25

        if len(message) > chunklen:
            message = message.encode("utf-8")[:chunklen - len(suffix)] \
                .decode("utf-8", "ignore") + suffix

        return message

    def message(self, target, message):
        message = self._autotruncate("PRIVMSG", target, message)

        @self.bot.defer_from_thread
        def _callback():
            super(Client, self).message(target, message)
            self._add_to_backlog(target, self.nickname, message)
            self._run_hooks("own_message", target, self.nickname, [target, message])

    def notice(self, target, message):
        message = self._autotruncate("PRIVMSG", target, message)

        @self.bot.defer_from_thread
        def _callback():
            super(Client, self).notice(target, message)
            self._run_hooks("own_notice", target, self.nickname, [target, message])

    def _run_hooks(self, name, target, origin, args=None, kwargs=None):
        @coroutine
        def _coro():
            nonlocal args, kwargs

            if args is None:
                args = []

            if kwargs is None:
                kwargs = {}

            for hook in self.bot.get_hooks(name):
                ctx = self.context_factory(hook.service, self.bot, self, target, origin)

                if not ctx.config.enabled:
                    continue

                try:
                    r = hook(ctx, *args, **kwargs)

                    if isinstance(r, Future):
                        r = yield r

                    if r is Service.EAT:
                        logging.debug("EAT suppressed further hooks.")
                        return Service.EAT
                except BaseException:
                    logger.exception("Hook processing failed")

        fut = _coro()
        @fut.add_done_callback
        def _callback(future):
            if future.exception() is not None:
                exc = future.exception()
                logger.error("Hook runner failed",
                             exc_info=(exc.__class__, exc, exc.__traceback__))

        return fut

    def _add_to_backlog(self, target, by, message):
        backlog = self.backlogs.setdefault(target, deque([]))
        backlog.appendleft((by, message))

        while len(backlog) > self.bot.config.core.max_backlog:
            backlog.pop()

    def on_invite(self, channel, by):
        self._run_hooks("invite", by, by, [channel, by])

    def on_join(self, channel, user):
        self._run_hooks("join", channel, user, [channel, user])

    def on_kill(self, target, by, reason):
        self._run_hooks("kill", by, by, [target, by, reason])

    def on_kick(self, channel, target, by, reason=None):
        self._run_hooks("kick", channel, by, [channel, target, by, reason])

    def on_mode_change(self, channel, modes, by):
        self._run_hooks("mode_change", channel, by, [channel, modes, by])

    def on_user_mode_change(self, modes):
        self._run_hooks("user_mode_change", None, self.nickname, [modes])

    def on_channel_message(self, target, by, message):
        self._add_to_backlog(target, by, message)
        self._run_hooks("channel_message", target, by, [target, by, message])

    def on_private_message(self, by, message):
        self._add_to_backlog(by, by, message)
        self._run_hooks("private_message", by, by, [by, message])

    def on_nick_change(self, old, new):
        self._run_hooks("nick_change", new, new, [old, new])

    def on_channel_notice(self, target, by, message):
        self._run_hooks("channel_notice", target, by, [target, by, message])

    def on_private_notice(self, by, message):
        self._run_hooks("private_notice", by, by, [by, message])

    def on_part(self, channel, user, message=None):
        self._run_hooks("part", channel, user, [channel, user, message])

    def on_topic_change(self, channel, message, by):
        self._run_hooks("topic_change", channel, by, [channel, message, by])

    def on_quit(self, user, message=None):
        self._run_hooks("quit", user, user, [user, message])

    def on_ctcp(self, by, target, what, contents):
        self._run_hooks("ctcp", by, by, [by, what, contents])

    def on_ctcp_action(self, by, what, contents):
        self._run_hooks("ctcp_action", by, by, [by, what, contents])
