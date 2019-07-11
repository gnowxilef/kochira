from concurrent.futures import ThreadPoolExecutor, Future

import collections
import functools
import imp
import importlib
import locale
import heapq
import logging
import multiprocessing
from peewee import SqliteDatabase
import signal
import yaml

from pydle import EventLoop, coroutine

from . import config
from .client import Client
from .db import database
from .scheduler import Scheduler
from .util import Expando
from .service import Service, BoundService, HookContext, Config as ServiceConfig
from .userdata import UserDataKVPair

from kochira import services


logger = logging.getLogger(__name__)


class ServiceConfigLoader(collections.Mapping):
    def __init__(self, bot, values):
        self.bot = bot
        self.configs = values
        self._cache = {}

    def _config_factory_for(self, name):
        if name not in self.bot.services:
            config_factory = ServiceConfig
        else:
            service = self.bot.services[name].service
            config_factory = service.config_factory

        return config_factory

    def __getitem__(self, name):
        config_factory = self._config_factory_for(name)

        # if we can't find the service name immediately, try removing
        # services.__name__ from the start
        if name.startswith(services.__name__) and name not in self.configs:
            name = name[len(services.__name__):]

        if name not in self._cache or \
            not isinstance(self._cache[name], config_factory):
            self._cache[name] = config_factory(self.configs[name])
        return self._cache[name]

    def __iter__(self):
        return iter(self.configs)

    def __len__(self):
        return len(self.configs)


def _config_class_factory(bot):
    lang, _ = locale.getdefaultlocale()

    service_config_loader = functools.partial(ServiceConfigLoader, bot)
    service_config_loader.get_default = lambda: {}

    class Config(config.Config):
        class Network(config.Config):
            autoconnect = config.Field(doc="Whether or not to autoconnect to this network.", default=True)

            nickname = config.Field(doc="Nickname to use.")
            username = config.Field(doc="Username to use.", default=None)
            realname = config.Field(doc="Real name to use.", default=None)

            hostname = config.Field(doc="IRC server hostname.")
            port = config.Field(doc="IRC server port.", default=None)
            password = config.Field(doc="IRC server password.", default=None)
            source_address = config.Field(doc="Source address to connect from.", default="")

            authenticated_userdata = config.Field(doc="Does user data need authentication?", default=True)
            response_format = config.Field(doc="How should responses be formatted?", default="{origin}: {message}")

            class TLS(config.Config):
                enabled = config.Field(doc="Enable TLS?", default=False)
                verify = config.Field(doc="Verify TLS connection?", default=True)
                certificate_file = config.Field(doc="TLS certificate file.", default=None)
                certificate_keyfile = config.Field(doc="TLS certificate key file.", default=None)
                certificate_password = config.Field(doc="TLS certificate password.", default=None)

            class SASL(config.Config):
                identity = config.Field(doc="SASL identity. Usually empty.", default=None)
                username = config.Field(doc="SASL username.", default=None)
                password = config.Field(doc="SASL password.", default=None)

            class Channel(config.Config):
                autojoin = config.Field(doc="Whether or not to autojoin to the channel.", default=True)
                password = config.Field(doc="Password for the channel, if any.", default=None)
                services = config.Field(doc="Mapping of per-channel service settings.", type=service_config_loader)
                acl = config.Field(doc="Mapping of per-channel access control lists.", type=config.Mapping(config.Many(str, is_set=True)))
                locale = config.Field(doc="Per-channel locale.", default=None)

            tls = config.Field(doc="TLS settings.", type=TLS, default=TLS())
            sasl = config.Field(doc="SASL settings.", type=SASL, default=SASL())

            channels = config.Field(doc="Mapping of channel settings.", type=config.Mapping(Channel))
            services = config.Field(doc="Mapping of per-client service settings.", type=service_config_loader)
            acl = config.Field(doc="Mapping of per-client access control lists.", type=config.Mapping(config.Many(str, is_set=True)))
            locale = config.Field(doc="Per-network locale.", default=None)

        class Core(config.Config):
            database = config.Field(doc="Database file to use", default="kochira.db")
            max_backlog = config.Field(doc="Maximum backlog lines to store.", default=10)
            max_workers = config.Field(doc="Max thread pool workers.", default=0)
            version = config.Field(doc="CTCP VERSION reply.", default="kochira IRC bot")
            locale_path = config.Field(doc="Path to locales.", default="/usr/share/locale")
            locale = config.Field(doc="Locale to use.", default=lang)

        core = config.Field(doc="Core configuration settings.", type=Core)
        clients = config.Field(doc="Clients to connect.", type=config.Mapping(Network))
        services = config.Field(doc="Services to load. Please refer to service documentation for setting this.", type=service_config_loader)

    return Config


class Bot:
    """
    The core bot.
    """

    def __init__(self, config_file="config.yml"):
        self.services = {}
        self.clients = {}
        self.event_loop = EventLoop()

        self.config_class = _config_class_factory(self)
        self.config_file = config_file

        self.stopping = False

        self.rehash()
        self._connect_to_db()

    def run(self):
        self.executor = ThreadPoolExecutor(self.config.core.max_workers or multiprocessing.cpu_count())
        self.scheduler = Scheduler(self)

        signal.signal(signal.SIGHUP, self._handle_sighup)

        self._load_services()
        self._connect_to_irc()

        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigterm)

        self.event_loop.run()

    def stop(self):
        self.stopping = True
        self.event_loop.stop()
        for service in list(self.services.keys()):
            self.unload_service(service)

    def connect(self, name):
        client = Client.from_config(self, name,
                                    self.config.clients[name])
        self.clients[name] = client
        return client

    def disconnect(self, name):
        client = self.clients[name]

        # schedule this for the next iteration of the ioloop so we can handle
        # pending messages
        self.event_loop.schedule(client.quit)

        del self.clients[name]

    def _connect_to_db(self):
        db_name = self.config.core.database
        database.initialize(SqliteDatabase(db_name, check_same_thread=True))
        logger.info("Opened database connection: %s", db_name)
        UserDataKVPair.create_table(True)

    def _connect_to_irc(self):
        for name, config in self.config.clients.items():
            if config.autoconnect:
                self.connect(name)

    def _load_services(self):
        for service, config in self.config.services.items():
            if config.autoload:
                try:
                    self.load_service(service)
                except:
                    pass # it gets logged

    def defer_from_thread(self, fn, *args, **kwargs):
        fut = Future()

        @coroutine
        def _callback():
            try:
                r = fn(*args, **kwargs)
            except Exception as e:
                fut.set_exception(e)
            else:
                if isinstance(r, Future):
                    try:
                        r = yield r
                    except Exception as e:
                        fut.set_exception(e)
                    else:
                        fut.set_result(r)
                else:
                    fut.set_result(r)

        self.event_loop.schedule(_callback)
        return fut

    def load_service(self, name, reload=False):
        """
        Load a service into the bot.

        The service should expose a variable named ``service`` which is an
        instance of ``kochira.service.Service`` and configured appropriately.
        """

        if name[0] == ".":
            name = services.__name__ + name

        # ensure that the service's shutdown routine is run
        if name in self.services:
            service = self.services[name].service
            service.run_shutdown(self)

        # we create an expando storage first for bots to load any locals they
        # need
        service = None

        try:
            module = importlib.import_module(name)

            if reload:
                module = imp.reload(module)

            if not hasattr(module, "service"):
                raise RuntimeError("{} is not a valid service".format(name))

            service = module.service
            self.services[service.name] = BoundService(service)

            service.run_setup(self)
        except:
            logger.exception("Couldn't load service %s", name)
            if service is not None:
                del self.services[service.name]
            raise

        logger.info("Loaded service %s", name)

    def unload_service(self, name):
        """
        Unload a service from the bot.
        """

        # if we can't find the service name immediately, try removing
        # services.__name__ from the start
        if name[0] == "." and name not in self.services:
            name = services.__name__ + name

        try:
            service = self.services[name].service
            service.run_shutdown(self)
            del self.services[name]
        except:
            logger.exception("Couldn't unload service %s", name)
            raise

    def get_hooks(self, hook):
        """
        Create an ordering of hooks to run.
        """

        return (hook for _, _, hook in heapq.merge(*[
            bound.service.hooks.get(hook, [])
            for bound in list(self.services.values())
        ]))

    def run_hooks(self, hook, *args, **kwargs):
        """
        Attempt to dispatch a command to all command handlers.
        """

        for hook in self.get_hooks(hook):
            ctx = HookContext(hook.service, self)

            try:
                r = hook(ctx, *args, **kwargs)

                if r is Service.EAT:
                    return Service.EAT
            except BaseException:
                logger.exception("Hook processing failed")

    def rehash(self):
        """
        Reload configuration information.
        """

        with open(self.config_file, "r") as f:
            self.config = self.config_class(yaml.load(f))

    def _handle_sighup(self, signum, frame):
        logger.info("Received SIGHUP; running SIGHUP hooks and rehashing")

        try:
            self.rehash()
        except Exception as e:
            logger.exception("Could not rehash configuration")

        self.run_hooks("sighup")

    def _handle_sigterm(self, signum, frame):
        if self.stopping:
            raise KeyboardInterrupt

        logger.info("Received termination signal; unloading all services")
        self.stop()
