## Core concepts of a task

A few core concepts define what is a task from a tukio's workflow point of view.
Many of those concepts were inspired from `asyncio.Future`:
* An instance of task has a unique ID (`uid`)
* Tasks can be interrupted on cancel or timeout
* Tasks give the opportunity to code _cleanup actions_ on timeout and cancel
* Cleanup actions may affect the result of a task
* A force-cancel method prevent _cancel cleanup actions_ from being executed
* A task has a few states: `pending`, `running` and `done`
* Sub-states of `done` are: `completed`, `timeout` and `cancelled`
* One can get the result returned by a task at any time
* One can get the state of a task at any time

_NOTE: the idea of enabling events throwing/receiving between tasks is still
in the air. But we can leave out this topic for now and focus on the primary
requirements we want in tasks._

## Implementation options

As a team, we stated that we would heavily rely on `asyncio`'s event loop.
That said, there are several ways to get things done with the concepts listed
above in mind:
1. A task is a simple coroutine
1. A task is a basic class (inherits from `object`)
1. A task is a class that inherits from `asyncio.Task`
1. A task is a class that inherits from `asyncio.Future`

A simple coroutine is not really convenient to implement cleanup actions and
handle stateful or exec context information; even wrapped in a `Future`
(possible though).
Hence it does not seem to be the right direction. Inheriting from a base class
is definitely the right way to work with tasks.

We could build our own base class `tukio.Task` from scratch and create our own
API. But as @thavel and @lerignoux objected, `asyncio.Future` and `asyncio.Task`
already implement most of what we want: **inheritance vs composition**, that's
the big deal!

That said, `asyncio.Task` wraps a coroutine in a future. Pretty well aligned
with the purpose of our tasks. However, the coroutine is scheduled as soon as
the class is created and `asyncio`'s event loop was not designed to dynamically
create a bunch of instances of various subclasses of `asyncio.Task`.

On the other hand, `asyncio.Future` also implements many methods expected in
the target `tukio.Task` class... But does not implement all the plumbing to
wrap a coroutine into a future (including the cancellation of a coroutine). For
sure, we won't reinvent the wheel!

There are two options left:
1. _Use_ `asyncio.Task` objects within `tukio.Task` (composition)
1. Inherit `tukio.Task` from `asyncio.Task`

Our first attempt was to use composition. Now, as we target an API that really
sticks to `asyncio.Task`'s API, let's try the other way.

The official `asyncio` documentation recommends to use `asyncio.ensure_future()`
or `BaseEventLoop.create_task()` to create tasks which always leads to create
an instance of `asyncio.Task` and schedule the wrapped coroutine at once.  
We can try the following: design `tukio.Task` to be instantiated directly and
override parent `asyncio.Task` so as to avoid the immediate scheduling of the
coroutine.

## tukio.Task()

See below a proposition of API for `tukio.Task`. Only additional methods are
described:

```python
class Task(asyncio.Task):
    def run(self, inputs):
        """Schedules the execution of the coroutine."""
    def execute(self, inputs):
        """Holds the code to run"""
    def on_timeout(self):
        """Callback executed on TimeoutError. Give the opportunity to cleanup
        the execution context and update the result of the task."""
    def on_cancel(self):
        """Callback executed on cancellation. Give the opportunity to cleanup
        the execution context and update the result of the task."""
```

## Outcome

After two experiments from @thavel and @lhavel, we found that, at last, we
should investigate the "_task as a coroutine_" implementation direction.
This is the simplest way to go.
We should move to a class-based implementation (inherited from `asyncio.Task`
or not) as soon as our tasks require class-level features (such as sharing a
global context and/or data between methods).
