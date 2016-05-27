from .holder import TaskHolder

"""
A join task is a regular task that have a new_call additional method that is called whenevera new parent call this task.
User can thus define custom behaviour on the arguments given (add them to the current ones ?)
and decide wether or not unlock the task ...
"""

class JoinTaskHolder(TaskHolder):

    async def new_call(self, *args, **kwargs):
        """
        Is called when the task is started and a new parents finished.
        """
