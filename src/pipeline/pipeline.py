import logging
import datetime
import queue
from queue import Queue
from typing import Any

from .queue import PipelineQueueThread

logger = logging.getLogger(__file__)


class PipelineSpecification:
    """PipelineSpecification

    Use for specify a Pipeline stage component (a part of a stage) to allow Pipeline
    to build an PipelineQueueThread from a simplify way

    Args:
        * run_callable (Callable[Any, dict] -> Any): Will be call as
        `run_callable(values, **options)`.
        * options (dict): a dictionnary took in the `queues_inputs` that will be update by
        `PipelineQueueThread.options`.
        * name (str|None): to rename the Thread.
    """

    def __init__(self, run_callable, options={}, name=None):
        self.run_callable = run_callable
        self.options = options
        self.name = name


class Pipeline:
    """Pipeline class

    Allow to instanciate a pipeline of workers over multiple threads
    that do an arbitrary treatment, and pass the result to the next thread.

    Note that if your last stage is including multiple output, the Pipeline will
    generate an additionnal stage that merge all output as a tuple and
    connect it to the Pipeline instance output.

    Additionnals args and kwargs will be pass to each Thread.

    Args:
        * stage_specs (list<PipelineSpecification|list<PipelineSpecification>>): 2 dimension
        list of specification.
        First dimension which stage is feeding which stage. Per exemple, `stage_specs[0]` will
        pass value to `stage_specs[1]`.
        Second dimension allow to a given stage to feed multiple stage.
        Per exemple, `stage_specs[0]` will pass value to all run_callable in `stage_specs[1]`.
        And `stage_specs[2]` will receive a tuple of values that come from `stage_specs[1]`.
        * queue_input (Queue): the input queue, default to a Queue(maxsize=10)
        * queue_output (Queue): the output queue, default to a Queue(maxsize=10)
        * daemon(bool): Enable deamon mode for subthread (default : True).
    """

    def __init__(
        self,
        stage_specs,
        *args,
        queue_input: Queue = None,
        queue_output: Queue = None,
        daemon: bool = True,
        **kwargs,
    ):
        self.stage_specs = [
            [stage_spec] if not isinstance(stage_spec, list) else stage_spec
            for stage_spec in stage_specs
        ]

        self.queue_input = Queue(maxsize=10) if queue_input is None else queue_input
        self.queue_output = Queue(maxsize=10) if queue_output is None else queue_output

        # Pull of all threads in the pipeline
        self._threads = []

        queues_inputs_next = [self.queue_input] * len(self.stage_specs[0])
        threads_feeders_next = None

        # For each STAGE
        # Init the bag of thread the current stage and the 'previous'
        # for link stage-to-stage
        pipeline_threads_stage: list[PipelineQueueThread] = []
        pipeline_threads_stage_previous: list[PipelineQueueThread] = []

        # ======================
        # Build user-defined stages
        # ======================
        for i, stage_spec in enumerate(self.stage_specs):
            # Compute how many output are required to link properly
            # to the next stage
            is_last_stage = i == len(self.stage_specs) - 1
            count_outputs_required = 1
            if not is_last_stage:
                count_outputs_required = len(self.stage_specs[i + 1])

            # Build current STAGE
            for j, spec in enumerate(stage_spec):

                # Build input/feeders for next stage from previous
                if pipeline_threads_stage_previous:
                    queues_inputs_next = [
                        t.queues_outputs[j] for t in pipeline_threads_stage_previous
                    ]
                    threads_feeders_next = pipeline_threads_stage_previous

                queues_outputs_next = [
                    Queue(maxsize=10) for n in range(count_outputs_required)
                ]

                # Build the `j`th stage component
                t = PipelineQueueThread(
                    *args,
                    name=spec.name,
                    queues_inputs=queues_inputs_next,
                    queues_outputs=queues_outputs_next,
                    run_callable=spec.run_callable,
                    options=spec.options,
                    threads_feeder=threads_feeders_next,
                    enable_self_terminate=False,
                    kill_check_delay=0.1,
                    daemon=daemon,
                    **kwargs,
                )

                # Register the thread and build next bag
                pipeline_threads_stage.append(t)
                self._threads.append(t)

            # Reset the bag of thread of the current stage
            pipeline_threads_stage_previous = pipeline_threads_stage
            pipeline_threads_stage = []

        # ======================
        # Connect to the Pipeline output
        # ======================
        stage_spec_last = self.stage_specs[-1]
        # Standardise to list
        if not isinstance(stage_spec_last, list):
            stage_spec_last = [stage_spec_last]
        # Connect to the Pipeline output
        if len(stage_spec_last) > 1:
            # Merge all outputs to a single output

            # Build input/feeders for next stage from previous
            queues_inputs_next = [
                t.queues_outputs[0] for t in pipeline_threads_stage_previous
            ]
            threads_feeders_next = pipeline_threads_stage_previous

            # Build last stage
            t = PipelineQueueThread(
                *args,
                name="thread-pipeline-merge-tail",
                queues_inputs=queues_inputs_next,
                queues_outputs=[self.queue_output],
                run_callable=lambda values: values,
                options={},
                threads_feeder=threads_feeders_next,
                enable_self_terminate=False,
                kill_check_delay=0.1,
                daemon=daemon,
                **kwargs,
            )
            self._threads.append(t)
        else:
            # Override the output to connect it to the pipeline output
            self._threads[-1].queues_outputs = [self.queue_output]

    def start(self):
        """Start all threads"""
        for t in self._threads:
            t.start()

    def stop(self, force=False):
        """Notify all threads that he must finish remaining jobs and stop himself.

        Args:
            * force (bool): if False, will await that the work is complete before terminate.
            If True, it will raise an exception to quicker terminate the thread without
            ensuring that the work is done.
        """
        for t in self._threads:
            t.stop(force)

    def join(self, timeout: float = None) -> bool:
        """Join all threads in the pipeline

        Args:
            * timeout (float): If not None, will raise a TimeoutError after awaiting `timeout`
            in seconds,
        Returns:
            (bool) As the opposite of `threading.Thread.join()`,
            this one will return True if joined before the timeout.
        """
        limit: datetime.datetime = None
        if timeout:
            limit = datetime.datetime.now() + datetime.timedelta(seconds=timeout)

        for t in self._threads:
            if timeout:
                # Reminder : `threading.Thread.join()` doesn't return value
                # using `threading.Thread.is_alive()` to check if timeout is
                # reach come from the documentation.
                t.join(timeout=(limit - datetime.datetime.now()).total_seconds())
                if t.is_alive():
                    return False
            else:  # pragma: no cover
                t.join()
        return True

    def has_alive(self) -> bool:
        """Allow to know if any is alive

        Returns:
            (bool) True if any thread of the pipeline is alive
        """
        return any([t.is_alive() for t in self._threads])

    def put(self, *args, **kwargs) -> None:
        """Shortcut for Pipeline.queue_input.put()"""
        self.queue_input.put(*args, **kwargs)

    def get(self, *args, **kwargs) -> None:
        """Shortcut for Pipeline.queue_output.get()"""
        return self.queue_output.get(*args, **kwargs)
