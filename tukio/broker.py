import asyncio
import inspect
import logging


log = logging.getLogger(__name__)


class _BrokerRegistry:

    """
    This class is designed to be a registry of all event brokers.
    """

    _registry = dict()

    @classmethod
    def get(cls, loop):
        """
        Get or create an event broker for the given event loop
        """
        try:
            broker = cls._registry[loop]
        except KeyError:
            broker = cls._registry[loop] = Broker(loop)
        return broker


class Broker(object):

    """
    The workflow engine has a global event broker that gathers external events
    (e.g. from the network) and internal events (e.g. from tasks) and schedules
    the execution of registered handlers.
    An event is fired with a topic. Registered handlers are executed each time
    an event is fired on that topic.

    Note: this class is not thread-safe. Its methods must be called from within
    the same thread as the thread running the event loop used by the workflow
    engine (including the broker)
    """

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._handlers = []

    def dispatch(self, data):
        """
        Passes an event (aka the data) received to each registered handler.
        """
        for handler in self._handlers:
            if asyncio.iscoroutinefunction(handler):
                asyncio.ensure_future(handler(data), loop=self._loop)
            else:
                self._loop.call_soon(handler, data)

    def register(self, coro_or_cb):
        """
        Registers a coroutine or a regular function to be executed upon
        receiving events.
        """
        if not inspect.isfunction(coro_or_cb):
            raise TypeError('{} is not a function'.format(coro_or_cb))
        self._handlers.append(coro_or_cb)

    def unregister(self, coro_or_cb):
        """
        Removes a coroutine or a regular function from the registered handlers.
        """
        try:
            self._handlers.remove(coro_or_cb)
        except ValueError:
            warn = '{} is not registered in broker, cannot unregister'
            log.warning(warn.format(coro_or_cb))


def get_broker(loop=None):
    """
    Returns the broker used in the event loop for the current context.
    There's only a single broker per event loop.
    """
    loop = loop or asyncio.get_event_loop()
    return _BrokerRegistry.get(loop)
