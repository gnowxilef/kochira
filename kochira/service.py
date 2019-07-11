import functools
import re
import logging
import locale
import bisect
import gettext
import os

from pydle import coroutine, Future

from .auth import has_permission, requires_permission
from .userdata import UserData
from . import config

from .util import Expando

logger = logging.getLogger(__name__)


class Config(config.Config):
    autoload = config.Field(doc="Autoload this service?", default=True)
    enabled = config.Field(doc="Enable this service?", default=True)


class BoundService:
    def __init__(self, service):
        self.service = service
        self.storage = Expando()
        self.contexts = {}


class HookContext:
    def __init__(self, service, bot, client=None, target=None, origin=None):
        self.service = service
        self.bot = bot
        self.client = client
        self.target = target
        self.origin = origin

        self._load_locale()

    @property
    def config(self):
        return self.config_for(self.service)

    def config_for(self, service):
        config = self.bot.config.services.get(service.name, service.config_factory())

        if self.client is not None:
            client_config = self.bot.config.clients[self.client.name]
            config = config.combine(client_config.services.get(service.name, service.config_factory()))

            if self.target is not None and self.target in client_config.channels:
                channel_config = client_config.channels[self.target]
                config = config.combine(channel_config.services.get(service.name, service.config_factory()))

        return config

    @property
    def storage(self):
        return self.service.binding_for(self.bot).storage

    def message(self, message):
        self.client.message(self.target, message)

    def respond(self, message):
        @coroutine
        def _coro():
            if (yield self.client._run_hooks(
                "respond", self.target, self.origin,
                [self.target, self.origin, message])) is not Service.EAT:
                self.message(self.client.config.response_format.format(
                    origin=self.origin,
                    message=message
                ))

        return _coro()

    def add_context(self, context):
        self.service.add_context(self.client, context, self.target)

    def remove_context(self, context):
        self.service.remove_context(self.client, context, self.target)

    def provider_for(self, name):
        for bound in self.bot.services.values():
            if name in bound.service.providers:
                return functools.partial(bound.service.providers[name], self)

        raise KeyError(name)

    @property
    def locale(self):
        locale = self.bot.config.core.locale

        if self.client is not None:
            client_config = self.bot.config.clients[self.client.name]
            locale = locale or client_config.locale

            if self.target is not None and self.target in client_config.channels:
                locale = locale or client_config.channels[self.target].locale

        return locale

    def _load_locale(self):
        lang, _ = locale.getdefaultlocale()
        languages = [self.locale, lang]

        try:
            self.t = gettext.translation("kochira",
                                         self.bot.config.core.locale_path,
                                         languages=languages)
        except IOError:
            self.t = gettext.NullTranslations()

    def lookup_user_data(self, who=None):
        if who is None:
            who = self.origin
        return UserData.lookup(self.client, who)

    def gettext(self, string):
        return self.t.gettext(string)

    _ = gettext

    def ngettext(self, sing, plur, n):
        return self.t.ngettext(sing, plur, n)


class Service:
    """
    A service provides the bot with additional facilities.
    """

    EAT = object()

    def __init__(self, name, doc=None):
        self.name = name
        self.doc = doc
        self.hooks = {}
        self.commands = set([])
        self.models = set([])
        self.tasks = []
        self.config_factory = Config
        self.on_setup = None
        self.on_shutdown = None
        self.providers = {}
        self.logger = logging.getLogger(self.name)

    def hook(self, hook, priority=0):
        """
        Register a hook with the service.
        """

        def _decorator(f):
            bisect.insort(self.hooks.setdefault(hook, []), (-priority, id(f), f))
            f.service = self
            return f

        return _decorator

    def command(self, pattern, priority=0, mention=False, strip=True,
                re_flags=re.I, eat=True, allow_private=True):
        """
        Register a command for use with the bot.

        ``mention`` specifies that the bot's nickname must be mentioned.
        """

        if pattern[-1] != "$":
            pattern += "$"
        pat = re.compile(pattern, re_flags)

        def _decorator(f):
            if not hasattr(f, "patterns"):
                f.patterns = set([])

            f.patterns.add((pattern, mention))

            @functools.wraps(f)
            @coroutine
            def _command_handler(ctx, target, origin, message):
                contexts = getattr(f, "contexts", set([]))
                if contexts:
                    # check for contexts
                    bound = self.binding_for(ctx.bot)

                    my_contexts = \
                        bound.contexts.get(ctx.client.name, {}) \
                            .get(target, set([])) | \
                        bound.contexts.get(ctx.client.name, {}) \
                            .get(None, set([]))

                    if not my_contexts & contexts:
                        return

                # check for permissions
                permissions = getattr(f, "permissions", set([]))

                hostmask = "{nickname}!{username}@{hostname}".format(
                    nickname=origin,
                    username=ctx.client.users[origin]["username"],
                    hostname=ctx.client.users[origin]["hostname"]
                )
                if not all(has_permission(ctx.client, hostmask, permission,
                                          target)
                           for permission in permissions):
                    return

                if strip:
                    message = message.strip()

                # check if we're either being mentioned or being PMed
                if mention and origin != target:
                    match = re.match(r"@?{}\W*\b(?P<rest>.+)".format(
                        re.escape(ctx.client.nickname)
                    ), message, re.IGNORECASE)

                    if match is None:
                        return

                    message = match.group("rest")

                match = pat.match(message)
                if match is None:
                    return

                kwargs = match.groupdict()

                for k, v in kwargs.items():
                    if k in f.__annotations__ and v is not None:
                        kwargs[k] = f.__annotations__[k](v)

                r = f(ctx, **kwargs)

                if isinstance(r, Future):
                    r = yield r

                if r is not None:
                    return r

                if eat:
                    return Service.EAT

            self.hook("channel_message", priority=priority)(_command_handler)

            if allow_private:
                self.hook("private_message", priority=priority)(
                    lambda client, origin, message: _command_handler(client,
                                                                     origin,
                                                                     origin,
                                                                     message))

            self.commands.add(f)
            return f

        return _decorator

    def task(self, f):
        """
        Register a new task. If the task is designed to be used as a timer,
        interval should be `None`.
        """

        f.service = self
        self.tasks.append(f)
        return f

    def config(self, factory):
        """
        Register a configuration factory.
        """
        self.config_factory = factory
        return factory

    def setup(self, f):
        """
        Register a setup function.
        """
        self.on_setup = f
        return f

    def shutdown(self, f):
        """
        Register a shutdown function.
        """
        self.on_shutdown = f
        return f

    def provides(self, name):
        """
        Register a provider function to expose to other services.
        """
        def _decorator(f):
            self.providers[name] = f
            return f
        return _decorator

    def _autocreate_models(self):
        for model in self.models:
            model.create_table(True)

    def run_setup(self, bot):
        """
        Run all setup functions for the service.
        """
        self._autocreate_models()

        ctx = HookContext(self, bot)

        if self.on_setup is not None:
            self.on_setup(ctx)

    def run_shutdown(self, bot):
        """
        Run all shutdown functions for the service.
        """
        # unschedule remaining work
        bot.scheduler.unschedule_service(self)

        ctx = HookContext(self, bot)

        if self.on_shutdown is not None:
            self.on_shutdown(ctx)

    def binding_for(self, bot):
        """
        Get the service binding.
        """
        return bot.services[self.name]

    def model(self, model):
        self.models.add(model)
        return model

    def add_context(self, client, context, target=None):
        self.binding_for(client.bot).contexts \
            .setdefault(client.name, {}) \
            .setdefault(target, set([])).add(context)

    def has_context(self, client, context, target=None):
        return context in self.binding_for(client.bot).contexts \
            .get(client.name, {}) \
            .get(target, set([]))

    def remove_context(self, client, context, target=None):
        self.binding_for(client.bot).contexts[client.name][target].remove(context)


def background(f):
    """
    Defer a command to run in the background.
    """

    f.background = True

    @functools.wraps(f)
    @coroutine
    def _inner(ctx, *args, **kwargs):
        result = yield ctx.bot.executor.submit(f, ctx, *args, **kwargs)

        @coroutine
        def _cont():
            nonlocal result

            if isinstance(result, Future):
                result = yield result
            return result

        # If we yielded the future from another thread (i.e. an executor
        # thread), we do this song and dance to force it back into the main
        # thread.
        return (yield ctx.bot.defer_from_thread(_cont))

    return _inner


def requires_context(context):
    """
    Require a context for the command.
    """

    def _decorator(f):
        if not hasattr(f, "contexts"):
            f.contexts = set([])
        f.contexts.add(context)

        return f
    return _decorator
