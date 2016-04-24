import logging
import asyncio


logger = logging.getLogger(__name__)


class Broker(object):
    """
    The workflow engine has a global event broker that gathers external
    (e.g. from the network) and internal events (e.g. from tasks) and schedules
    the execution of registered handlers.
    A event is fired with a topic. Registered handlers are executed each time
    an event is fired on that topic.
    """
    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._handlers = {}

    def _ensure_coroutine(self, callback):
        if not asyncio.iscoroutinefunction(callback):
            return asyncio.coroutine(callback)
        return callback

    async def fire(self, topic, data):
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

    def register(self, topic, handler):
        """
        Register a handler to be executed upon receiving events in a given
        topic.
        """
        coro = self._ensure_coroutine(handler)
        try:
            self._handlers[topic].append(coro)
        except KeyError:
            self._handlers[topic] = [coro]
