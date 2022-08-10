import inspect
import logging
import threading
import queue
from queue import Queue
from typing import Callable, Any

from .exception import StopException

logger = logging.getLogger(__file__)


class StoppableQueueThread(threading.Thread):
    """Basic Thread with stop flag and queue for data inputs and outputs

    Is design to be override as a regular `threading.Thread`.

    This class implement 3 main features:
        1. An input and an output queue available in the `threading.Thread.run()`
        that you will override.
        2. The instance take an optionnal `threads_feeder` argument, which is a list of Thread,
        which are monitored.
        If the `threads_feeder` die, and the queues_inputs is empty, this thread will terminate.
        Behavior that you can enable/disable by `enable_self_terminate` argument.
        3. Finally, you can stop the thread peacefully by calling stop() on the instance,
        even from another thread.

    Args:
        * queues_inputs (list[Queue]): the input queue list.
        * queues_outputs (list[Queue]): the output queue list.
        * threads_feeder (list[threading.Thread]|None): a parent Thread. This thread will terminate if
        the parent is dead and the input queue is empty. Must be of the same size than `queues_inputs`.
        * enable_self_terminate (bool): On true, Enable self terminate if the `threads_feeder`
        is dead, and the queues_inputs is empty.
        * kill_check_delay (float): amount in seconds that thread will wait (both on output
        and input queue) before check if it must terminate (default to 0.1)
    """

    def __init__(
        self,
        queues_inputs: list[Queue],
        queues_outputs: list[Queue],
        *args,
        threads_feeder: list[threading.Thread] = None,
        enable_self_terminate: bool = True,
        kill_check_delay: float = 0.1,
        **kwargs,
    ):
        # threading.Thread.__init__(self)
        super().__init__(*args, **kwargs)

        # `queues_inputs` check
        assert isinstance(queues_inputs, list), "`queues_inputs` must be a list"
        assert all(
            [isinstance(q, Queue) for q in queues_inputs]
        ), "`queues_inputs` items must be Queue"

        # `queues_outputs` check
        assert isinstance(queues_outputs, list), "`queues_outputs` must be a list"
        assert all(
            [isinstance(q, Queue) for q in queues_outputs]
        ), "`queues_outputs` items must be Queue"

        # `threads_feeder` check
        if __debug__ and threads_feeder is not None:
            assert isinstance(
                threads_feeder, list | tuple
            ), "`threads_feeder` must be a list or a tuple"
            assert all(
                [isinstance(t, threading.Thread) for t in threads_feeder]
            ), "`threads_feeder` items must be Thread"
            assert len(threads_feeder) == len(
                queues_inputs
            ), "`threads_feeder` must be same length than `queues_inputs`"

        # primitivs arguments check
        assert isinstance(kill_check_delay, float)
        assert isinstance(enable_self_terminate, bool)

        self.queues_inputs = queues_inputs
        self.queues_outputs = queues_outputs
        self.threads_feeder = threads_feeder
        self.kill_check_delay = kill_check_delay
        self.enable_self_terminate = enable_self_terminate

        self.STOP_FLAG = False

    def stop(self, force: bool = False) -> None:
        """Notify the thread that he must finish remaining jobs and stop himself.

        Args:
            * force (bool): if False, will await that the work is complete before terminate.
            If True, it will raise an exception to quicker terminate the thread without
            ensuring that the work is done.
        """
        if force:
            self.STOP_FLAG = True
        else:
            self.enable_self_terminate = True

    def are_feeders_alive(self) -> bool:
        """Return True if feeder thread is set and alive"""
        return self.threads_feeder is not None and all(
            [t.is_alive() for t in self.threads_feeder]
        )

    def is_stopping(self) -> bool:
        """Return True if the Thread must stop"""
        return self.STOP_FLAG or (
            self.enable_self_terminate
            and not self.are_feeders_alive()
            and all([q.empty() for q in self.queues_inputs])
            and all([q.empty() for q in self.queues_outputs])
        )

    def put(self, value) -> None:
        """Attempt to put until succeed
        Raise a StopException if attempt to put while stop force signal was send
        """

        cursor = 0
        while cursor < len(self.queues_outputs):
            if self.STOP_FLAG:
                # We're not stop on `self.is_stopping`, to allow to put the last item
                raise StopException(
                    f"{self.__class__.__name__}.put : the thread must be stopped"
                )
            try:
                self.queues_outputs[cursor].put(
                    value,
                    block=True,
                    timeout=self.kill_check_delay,
                )
                cursor += 1
            except queue.Full:
                pass

    def get(self) -> tuple | Any:
        """Attempt to get until succeed
        Raise a StopException if attempt to put while stop force signal was send

        Returns:
            tuple of values if multiple inputs queues, pure value if 1 input queue
        """
        values_buffer: list = []
        cursor = 0
        while cursor < len(self.queues_inputs):
            # Check for terminal signal
            if self.is_stopping():
                raise StopException(
                    f"{self.__class__.__name__}.get : the thread must be stopped"
                )
            try:
                values_buffer.append(
                    self.queues_inputs[cursor].get(timeout=self.kill_check_delay)
                )
                cursor += 1
            except queue.Empty:
                pass
        return tuple(values_buffer) if len(values_buffer) > 1 else values_buffer[0]

    def task_done(self) -> None:
        """Notify all inputs queue that the last task is complete"""
        for q in self.queues_inputs:
            q.task_done()


class PipelineQueueThread(StoppableQueueThread):
    """A StoppableQueueThread that take the content of the run() as a `run_callable` argument.

    Will await that all queues in `queues_inputs` have at last 1 value then pull 1 for each on
    them, and pass it as tuple as first argument of `run_callable`.

    The return value from `run_callable` is put to the output queue.

    `PipelineQueueThread.options` will be pass a keyword argument to the `run_callable` on each
    call.

    Args:
        * queues_inputs (list[Queue]): the input queue. Will await that all queues have at last 1 value
        then pull 1 for each on them, and pass it as tuple as first argument of `run_callable`.
        Queue order is respected in the tuple value order.
        * queues_outputs (list[Queue]): the output queue. Result from `run_callable` will be put in
        each of them.
        * run_callable (Callable[Any, dict] -> Any): Will be call as `run_callable(values, **options)`.
        Can be a generator.
        * options (dict): a dictionnary took in the `queues_inputs` that will be update by
        `PipelineQueueThread.options`.
        * threads_feeder (threading.Thread): a parent Thread. This thread will terminate if the
        parent is dead and the input queue is empty.
        * enable_self_terminate (bool): On true, Enable self terminate if the `threads_feeder`
        is dead, and the queues_inputs is empty.
        * kill_check_delay (float): amount in seconds that thread will wait (both on output and
        input queue) before check if it must terminate (default to 0.1).
    """

    def __init__(
        self,
        *args,
        run_callable: Callable[[Any, ...], Any],
        options: dict[str, Any] = {},
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        assert callable(run_callable)
        assert isinstance(options, dict)

        self.run_callable = run_callable
        self.options = options

    def run(self):
        """Pull until get a value,
        On value received, call the run_callable on value and options
        put to the output the result
        """

        while not self.is_stopping():
            try:
                # Get next set of values + compute (or get generator) + put
                run_callable_output = self.run_callable(self.get(), **self.options)
                if inspect.isgenerator(run_callable_output):
                    for item in run_callable_output:
                        self.put(item)
                else:
                    self.put(run_callable_output)

                # Notify the queue that the item is treated
                self.task_done()
            except StopException:
                return
            except KeyboardInterrupt:  # pragma: no cover
                return
            except Exception:
                self.stop(force=True)
                logger.exception(f"{self.__class__.__name__}.run : an error occurs")
                return
