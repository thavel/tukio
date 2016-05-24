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


class BrokerDeleteTopicError(Exception):
    pass


class Broker(object):

    """
    The workflow engine has a global event broker that gathers external events
    (e.g. from the network) and internal events (e.g. from tasks) and schedules
    the execution of registered handlers.
    If an event is disptached with a topic, all handlers registered with that
    topic will be executed. Else, all handers (whatever the topic) will be
    executed.

    Note: this class is not thread-safe. Its methods must be called from within
    the same thread as the thread running the event loop used by the workflow
    engine (including the broker)
    """

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._handlers = dict()
        self._topics = dict()

    def add_topic(self, topic):
        """
        Adds a new topic to register handlers with.
        """
        if topic in self._topics:
            return
        self._topics[topic] = set()

    def del_topic(self, topic):
        """
        Deletes an existing topic. Raises `BrokerDeleteTopicError` if handlers
        are still registered with that topic of if the topic doesn't exist.
        """
        try:
            handlers = self._topics[topic]
        except KeyError:
            raise BrokerDeleteTopicError("topic {} doesn't "
                                         "exist".format(topic))
        if handlers:
            raise BrokerDeleteTopicError("handlers still registered "
                                         "with {}".format(topic))
        del self._topics[topic]

    def dispatch(self, data, topic=None):
        """
        Passes an event (aka the data) received to each registered handler.
        If topic is not None, the event is dispatched to handlers registered
        to that topic only. Else it is dispatched to all handlers.
        Even if a handler was registered multiple times in multiple topics, it
        will be executed only once per call to `dispatch()`.
        """
        if topic is not None:
            try:
                handlers = self._topics[topic]
            except KeyError:
                handlers = set()
        else:
            handlers = self._handlers
        for handler in handlers:
            if asyncio.iscoroutinefunction(handler):
                asyncio.ensure_future(handler(data), loop=self._loop)
            else:
                self._loop.call_soon(handler, data)

    def register(self, coro_or_cb, topic=None):
        """
        Registers a handler (i.e. a coroutine or a regular function) to be
        executed upon receiving new events.
        If topic is not None, the handler will receive all events dispatched in
        that topic only. Else it will receive all events dispatched by the
        broker.
        A handler can be registered multiple times in different topics. But a
        handler can neither be registered as global when already per-topic nor
        per-topic when already global.
        """
        if not inspect.isfunction(coro_or_cb):
            raise TypeError('{} is not a function'.format(coro_or_cb))
        # Handler already registered with topics?
        curr_topics = self._handlers.get(coro_or_cb)
        if topic is not None:
            if curr_topics == set():
                raise ValueError('{} already registered as global'
                                 ' handler'.format(coro_or_cb))
            # Topics must have been added prior to registering handlers
            self._topics[topic].add(coro_or_cb)
            try:
                self._handlers[coro_or_cb].add(topic)
            except KeyError:
                self._handlers[coro_or_cb] = {topic}
        else:
            if curr_topics:
                raise ValueError('{} already registered with'
                                 ' topics {}'.format(coro_or_cb, curr_topics))
            self._handlers[coro_or_cb] = set()

    def unregister(self, coro_or_cb, topic=None):
        """
        Unregisters a per-topic or a global handler. If topic is None, all
        registrations (even per-topic) of that handler will be removed.
        """
        if topic is not None:
            self._topics[topic].remove(coro_or_cb)
            self._handlers[coro_or_cb].remove(topic)
            if not self._handlers[coro_or_cb]:
                del self._handlers[coro_or_cb]
        else:
            curr_topics = self._handlers[coro_or_cb]
            for ct in curr_topics:
                self._topics[ct].remove(coro_or_cb)
            del self._handlers[coro_or_cb]


def get_broker(loop=None):
    """
    Returns the broker used in the event loop for the current context.
    There's only a single broker per event loop.
    """
    loop = loop or asyncio.get_event_loop()
    return _BrokerRegistry.get(loop)
