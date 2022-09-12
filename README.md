# Pypeline
## What does this library ?
It give a **very simple way** to multitreat process over all your processors.
I use it to treat very large dataset in just-in-time way (cross process stream).

## Pipeline your treatment
Supposing 3 python treatement A B C that have dependancies A←B←C.
In the normal way, single processing, A execute first, then B, then C.

With the library, by split into chunk your input, you can do A(chunk) in parallele of B(chunk-1) and C(chunk-2).

Which result that the time of the whole operation is equal to the largest time use by A/B/C, instead of the sum as A+B+C.

## More complexe pipeline
The library allow allow to have multiple different process output.
Exemple: A←B1, A←B2, B1←C and B2←C
So B1 and B2 will receive the same inputs, and C will receive a tuple of result of B1 and B2 as input, the pipeline awaiting that both
B1 and B2 did have a value to give to proceed.

## Queueing included
Each step include a Queue of output/input of size 10 by default overridable by the Pipeline constructor.
It allow faster execution when step take the approximate same time to process.

# Get started
`pip install pipeline-turing-system`

It has no dependancy excluding python. Tested with Python3.10 but any modern python version should handle it without issue.

This library is very simple.

How have [exemples](https://github.com/turing-system/pipeline/blob/master/tests/test_pipeline.py) usage in tests, and documentation directly in classes doctring.

You only need to check [`pipeline.pipeline.PipelineSpecification` and `pipeline.pipeline.Pipeline`](https://github.com/turing-system/pipeline/blob/master/src/pipeline/pipeline.py) to do your magic.

You will figure it out without problem.

# Licence (TL;DR; version)
This small program is safe to include in your projects, even commercial, and doesn't require to propagate the licence to projects that include it.
Details in the (LICENCE.md)[./LICENCE.md] (MIT licence)
