# Tukio

Tukio is an event based workflow generator library built around [asyncio](https://docs.python.org/3/library/asyncio.html).

## Concepts

It is intended as a highly configurable workflow execution engine, able to run a succession of tasks from an simple event.
Our current goal will include :
* A clean python API to generate, run and keep track of multiple concurrent workflows
* An event-driven workflow execution, letting us push actions into a running workflow and do something upon reaching a certain state for a particular task
* A way to merge multiple task results
