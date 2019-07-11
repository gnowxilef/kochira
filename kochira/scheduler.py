import threading
import logging
from pydle import Future

from .service import HookContext


logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, bot):
        self.bot = bot

        self.timeouts = {}
        self.periods = {}

        self._next_period_id = 0

    def _error_handler(self, future):
        exc = future.exception()
        if exc is not None:
            logging.error("Background task error",
                          exc_info=(exc.__class__, exc, exc.__traceback__))

    def schedule_after(self, _time, _task, *_args, **_kwargs):
        """
        Schedule a task to run after a given amount of time.
        """
        logger.info("Scheduling %s.%s in %s", _task.service.name, _task.__name__, _time)

        timeout = None
        ctx = HookContext(_task.service, self.bot)

        def _handler():
            # ghetto-ass code for removing the timeout on completion
            nonlocal timeout

            r = _task(ctx, *_args, **_kwargs)

            if isinstance(r, Future):
                r.add_done_callback(self._error_handler)

            self.timeouts[_task.service.name].remove(timeout)

        timeout = self.bot.event_loop.schedule_in(_time, _handler)

        self.timeouts.setdefault(_task.service.name, set([])).add(timeout)
        return (_task.service.name, timeout)

    def schedule_every(self, _interval, _task, *_args, **_kwargs):
        """
        Schedule a task to run at every given interval.
        """
        logger.info("Scheduling %s.%s every %s", _task.service.name, _task.__name__, _interval)

        period_id = self._next_period_id
        self._next_period_id += 1

        ctx = HookContext(_task.service, self.bot)

        def _handler():
            if period_id not in self.periods.get(_task.service.name, set([])):
                return False

            r = _task(ctx, *_args, **_kwargs)

            if isinstance(r, Future):
                r.add_done_callback(self._error_handler)

            return True

        self.bot.event_loop.schedule_periodically(_interval, _handler)
        self.periods.setdefault(_task.service.name, set([])).add(period_id)
        return (_task.service.name, period_id)

    def unschedule_timeout(self, timeout):
        service_name, timeout = timeout
        self.bot.event_loop.unschedule(timeout)
        self.timeouts[service_name].remove(timeout)

    def unschedule_period(self, period):
        service_name, period_id = period
        self.periods[service_name].remove(period_id)

    def unschedule_service(self, service):
        logger.info("Unscheduling all tasks for service %s", service.name)

        if service.name in self.timeouts:
            for timeout in list(self.timeouts[service.name]):
                self.unschedule_timeout((service.name, timeout))

            del self.timeouts[service.name]

        if service.name in self.periods:
            del self.periods[service.name]
