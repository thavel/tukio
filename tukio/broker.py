import asyncio
import logging


logger = logging.getLogger(__name__)


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
    """

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._handlers = dict()

    def fire(self, topic, data):
        """
        Passes an event (aka the data) received to each registered handler.
        """
        try:
            handlers = self._handlers[topic]
        except KeyError:
            logger.warning("No handler registered for topic '{}'".format(topic))
            return
        for handler in handlers:
            asyncio.ensure_future(handler(data), loop=self._loop)

    def register(self, handler, topic=None):
        """
        Register a handler to be executed upon receiving events in a given
        topic.
        """
        if not asyncio.iscoroutinefunction(handler):
            raise ValueError('handler must be a coroutine')

        try:
            self._handlers[topic].append(handler)
        except KeyError:
            self._handlers[topic] = [handler]


def get_broker(loop=None):
    """
    Returns the broker used in the event loop for the current context.
    There's only a single broker per event loop.
    """
    loop = loop or asyncio.get_event_loop()
    return _BrokerRegistry.get(loop)
