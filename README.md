# Tukio

[![Circle CI](https://img.shields.io/circleci/project/optiflows/tukio/master.svg)](https://circleci.com/gh/optiflows/tukio)
[![pypi version](http://img.shields.io/pypi/v/tukio.svg)](https://pypi.python.org/pypi/tukio)
[![python versions](https://img.shields.io/pypi/pyversions/tukio.svg)](https://pypi.python.org/pypi/tukio/)

Tukio is an event based workflow generator library built around [asyncio](https://docs.python.org/3/library/asyncio.html).

## Concepts

It is intended as a highly configurable workflow execution engine, able to run a succession of tasks from an simple event.
Our current goal will include :
* A clean python API to generate, run and keep track of multiple concurrent workflows
* An event-driven workflow execution, letting us push actions into a running workflow and do something upon reaching a certain state for a particular task
* A way to merge multiple task results

## Asyncio loop task factory
The asyncio task factory as been changed within the library using `set_task_factory`. Please make sure your project does not need to switch it itself too.
