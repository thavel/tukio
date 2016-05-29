import asyncio
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
    A handler can be registerd as global (receives all events) or per-topic
    (receives events only from its registered topics).
    If an event is disptached with a topic, all handlers registered with that
    topic will be executed as well as all global handlers. Else, only global
    handlers will be executed.

    Note: this class is not thread-safe. Its methods must be called from within
    the same thread as the thread running the event loop used by the workflow
    engine (including the broker)
    """

    def __init__(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        self._handlers = dict()
        self._topics = {None: set()}

    def dispatch(self, data, topic=None):
        """
        Passes an event (aka the data) received to the right handlers.
        If topic is not None, the event is dispatched to handlers registered
        to that topic only + global handlers. Else it is dispatched to global
        handlers only.
        Even if a handler was registered multiple times in multiple topics, it
        will be executed only once per call to `dispatch()`.
        """
        # Always include the set of global handlers (they receive all events)
        global_handlers = self._topics[None]
        if topic is not None:
            try:
                handlers = self._topics[topic]
            except KeyError:
                handlers = set()
            handlers = handlers | global_handlers
        else:
            handlers = global_handlers
        for handler in handlers:
            if asyncio.iscoroutinefunction(handler):
                asyncio.ensure_future(handler(data), loop=self._loop)
            else:
                self._loop.call_soon(handler, data)

    def register(self, coro_or_cb, topic=None, everything=False):
        """
        Registers a handler (i.e. a coroutine or a regular function/method) to
        be executed upon receiving new events.
        If topic is not None, the handler will receive all events dispatched in
        that topic only. Else it will receive all events dispatched by the
        broker (whatever the topic including None).
        A handler can be registered multiple times in different topics. But a
        handler can neither be registered as global when already per-topic nor
        registered as per-topic when already global.
        """
        if not callable(coro_or_cb):
            raise TypeError('{} is not a callable object'.format(coro_or_cb))
        # Handler already registered with topics?
        curr_topics = self._handlers.get(coro_or_cb)
        if topic is not None:
            if curr_topics == set():
                raise ValueError('{} already registered as global'
                                 ' handler'.format(coro_or_cb))
            # Update or add topic's registration
            try:
                self._topics[topic].add(coro_or_cb)
            except KeyError:
                self._topics[topic] = {coro_or_cb}
            # Update or add handler's registration
            try:
                self._handlers[coro_or_cb].add(topic)
            except KeyError:
                self._handlers[coro_or_cb] = {topic}
        else:
            if curr_topics:
                raise ValueError('{} already registered with'
                                 ' topics {}'.format(coro_or_cb, curr_topics))
            self._handlers[coro_or_cb] = set([None])
            self._topics[None].add(coro_or_cb)

    def _discard_topic(self, coro_or_cb, topic):
        """
        Try to cleanup the dict of handlers and topics.
        """
        self._topics[topic].discard(coro_or_cb)
        if topic is not None and not self._topics[topic]:
            del self._topics[topic]

    def unregister(self, coro_or_cb, topic=None):
        """
        Unregisters a per-topic or a global handler. If topic is None, all
        registrations (global or per-topic) of that handler will be removed.

        This is a fault-tolerant implementation so as to ensure the runtime
        context of the broker will remain clean at all times. It re-raises
        KeyError exceptions after context cleanup.
        """
        ke_exc = None
        if topic is not None:
            # Try to cleanup the dict of topics first
            try:
                self._discard_topic(coro_or_cb, topic)
            except KeyError as exc:
                ke_exc = exc
            finally:
                # Try to cleanup the dict of handlers anyway
                try:
                    self._handlers[coro_or_cb].discard(topic)
                    if not self._handlers[coro_or_cb]:
                        del self._handlers[coro_or_cb]
                except KeyError as exc:
                    # explicitely chain exceptions
                    raise exc from ke_exc
        else:
            # No further cleanup to perform if a KeyError is raised here
            curr_topics = self._handlers[coro_or_cb]
            for ct in curr_topics:
                try:
                    self._discard_topic(coro_or_cb, ct)
                except KeyError as exc:
                    ke_exc = exc
            del self._handlers[coro_or_cb]
        # Re-raise the KeyError exception if something went wrong
        if ke_exc is not None:
            raise ke_exc


def get_broker(loop=None):
    """
    Returns the broker used in the event loop for the current context.
    There's only a single broker per event loop.
    """
    loop = loop or asyncio.get_event_loop()
    return _BrokerRegistry.get(loop)
