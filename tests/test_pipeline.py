import unittest
import queue

from pipeline import (
    Pipeline,
    PipelineSpecification,
)


class TestPipelineCommonUse(unittest.TestCase):
    """Test some common use case of the library"""

    def test_normal_use_multiple_output(self):
        """Test that pipeline is working fine with a same level
        of complexity describe in the documentation.

        We're testing a multiple stages pipeline with
        multiple components stage, that end to multiple
        output case.

        Test that should return an ordered tuple.
        """

        p = Pipeline(
            [
                PipelineSpecification(lambda x: x + 3, name="t1"),
                [
                    PipelineSpecification(lambda x: x + 5, name="t2"),
                    PipelineSpecification(lambda x: x + 7, name="t3"),
                    PipelineSpecification(lambda x: x + 9, name="t4"),
                ],
                PipelineSpecification(sum, name="t5"),
                [
                    PipelineSpecification(lambda x: x + 11, name="t6"),
                    PipelineSpecification(lambda x: x + 13, name="t7"),
                    PipelineSpecification(lambda x: x + 17, name="t8"),
                ],
            ]
        )
        p.start()
        p.put(1)
        output: tuple = None
        try:
            output = p.get(timeout=1)
        except queue.Empty:
            pass
        p.stop(force=False)

        self.assertIsNotNone(output, "The values doesn't pass through the pipeline")
        self.assertIsInstance(output, tuple, "Bad output type")
        self.assertTupleEqual(
            output,
            (44, 46, 50),
            "Good output type, but bad content",
        )

    def test_normal_use_single_output(self):
        """Test that pipeline is working fine with a same level
        of complexity describe in the documentation.

        We're testing a multiple stages pipeline with
        multiple components stage, that end to a single
        output case.

        Test that should return the final computed value (not a tuple).
        """

        p = Pipeline(
            [
                PipelineSpecification(lambda x: x + 3, name="t1"),
                [
                    PipelineSpecification(lambda x: x + 5, name="t2"),
                    PipelineSpecification(lambda x: x + 7, name="t3"),
                    PipelineSpecification(lambda x: x + 9, name="t4"),
                ],
                PipelineSpecification(sum, name="t5"),
                [
                    PipelineSpecification(lambda x: x + 11, name="t6"),
                    PipelineSpecification(lambda x: x + 13, name="t7"),
                    PipelineSpecification(lambda x: x + 17, name="t8"),
                ],
                PipelineSpecification(sum, name="t9"),
            ]
        )
        p.start()
        p.put(1)
        output: int = None
        try:
            output = p.get(timeout=1)
        except queue.Empty:
            pass
        p.stop()

        self.assertIsNotNone(output, "The values doesn't pass through the pipeline")
        self.assertIsInstance(output, int, "Bad output type")
        self.assertEqual(
            output,
            140,
            "Good output type, but bad content",
        )

    def test_generator_use_single_output(self):
        """Test that pipeline is working fine with a same level
        of complexity describe in the documentation.

        We're testing a multiple stages pipeline with
        multiple components stage, that end to a single
        output case.

        Test that should return the final computed value (not a tuple).
        """

        def generator(x):
            for i in range(3):
                yield x + i

        p = Pipeline(
            [
                PipelineSpecification(generator, name="t1"),
                [
                    PipelineSpecification(lambda x: x + 5, name="t2"),
                    PipelineSpecification(lambda x: x + 7, name="t3"),
                    PipelineSpecification(lambda x: x + 9, name="t4"),
                ],
                PipelineSpecification(sum, name="t9"),
            ]
        )
        p.start()
        p.put(1)
        output: list = None
        try:
            output = [p.get(timeout=1) for n in range(3)]
        except queue.Empty:
            pass

        p.stop(force=True)

        self.assertIsNotNone(output, "The values doesn't pass through the pipeline")
        self.assertIsInstance(output, list, "Bad output type")
        self.assertListEqual(
            output,
            [24, 27, 30],
            "Good output type, but bad content",
        )

    def test_stop_force(self):
        """Test that pipeline is working fine with a same level
        of complexity describe in the documentation.

        We're testing a multiple stages pipeline that take time
        that we're force stop.

        We're testing `join()` by the way
        """

        import time

        def that_take_time(v):
            time.sleep(0.1)
            return v

        p = Pipeline(
            [PipelineSpecification(that_take_time, name=str(i)) for i in range(100)]
        )
        p.start()
        p.queue_input.put(1)
        p.stop(force=True)
        self.assertTrue(p.join(timeout=2))
        self.assertFalse(p.has_alive())
