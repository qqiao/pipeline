// Copyright 2022 Qian Qiao
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*

Package pipeline contains an implementation of the Pipeline concurrency pattern
with Go 1.18 generics support.

Background

This Pipeline concurrency pattern is inspired by the blog post from the Go dev
team here: https://go.dev/blog/pipelines.

This library takes the overall idea of the blog post and makes it easier to
use by implementing it with support of generics, introduced in Go 1.18.

Additional care has also been taken in the design of the API to ensure that both
connecting multiple pipelines and creating multi-stage pipelines are as easy
as possible.

Basic Concepts

To better understand how a pipeline works we start by taking a look at the
components that make up a pipeline.

A Producer is a channel from which a pipeline gets its inputs from, a pipeline
will continue to read from the producer channel until it is closed or the
context is cancelled . More on the cancellation of the context later in the
Stopping Short section.

A Consumer is any struct that consumes a Producer. This library provides an
interface with the definition of a Consumer. Anything that matches the
signature is implicitly a consumer.

Since a pipeline itself is obviously a consumer of some input, and a pipeline
can be considered a producer since it returns a channel where the results are
sent into, chaining pipelines is both logically natural and as been made easy
to do so. We will discuss it in more detail in the Chaining Pipelines section.

A ConsumerFunc, which is a function that takes a channel, into which the
results of the pipeline are sent, as its sole argument. Since a channel where
values are sent into is also the definition of a Producer, you can consider a
ConsumerFunc as this:
    type ConsumerFunc[I any] func(Producer[I])

Consuming a Pipeline

There are multiple ways of consuming the output of a pipeline.

1. Create a Consumer, and have the Consumes function take the returned channel
of a pipeline. When we chain pipelines, this is the most effortless way.

2. Create a ConsumerFunc, pass it to the pipeline using the WithConsumer
method. The advantage of consuming the pipeline results this way is that
the ConsumerFunc can be easily written and unit tested without even needing
a pipeline.

3. Directly taking the returned channel of the Produces method and reading from
it. Although this approach requires the least code to be written, we strongly
encourage applications to not use this approach and instead use methods 1 and
2, as those two methods allow modular and unit-testable code to be written.

More advanced uses of the Consumer and Producer pattern will be discussed
further in the Chaining Pipelines section.

Stages

Stages is heart of the pipeline. While the API of a Stage look extremely
similar to that of a Pipeline, the actual multiplexing of the workers and the
final collation of the results are done by the stage. Therefore, the NewStage
function requires additional parameters to control the multiplexing behaviours
of the stage.

Each stage must have a Worker function. A worker is just a simple function that
takes an input and returns an output. The Stage will take care of the
parallelization workers and the combination of the results.vSince multiple
Worker instances will be created, Worker functions are expected to be
thread-safe to prevent any unpredictable results.

The workerPoolSize parameter defines the upper bound of the number of workers.

All Worker goroutines are lazily created. A new worker goroutine is created
when a new input is read from the producer, until workerPoolSize is reached.
Given that goroutines are cheap to create, in order to keep the implementation
as simple as possible, no worker re-use will happen in this growing phase.

Once workerPoolSize is reached, worker goroutines will compete for the inputs
from the producer until the pipeline is done. The pool will not auto shrink for
simplicity reasons.

The bufferSize parameter defines the buffer size of the output channel. This
allows the processing to start without having to block wait on the consumer to
start reading.

Chaining Pipelines

A Pipeline also has a set of APIs to make pipeline chaining straight-forward.
While connecting multiple pipelines will produce identical results as a single
pipeline with all the constituent stages, there are advantages of having
composable pipelines. The main advantage of making pipelines composable is so
that pipelines from different authors and libraries can be easily re-used.

Without the ability to directly connect pipelines, authors would need to
explicitly create a Stage instance for each stage of their Pipeline, and make
them public, instead of allowing the Pipeline's AddStage API to do it behind
the scenes automatically. More ever, users will also need to ensure that these
stages are added to their own pipeline in the correct order.

With composable pipelines, this is no longer an issue. A Pipeline can be made
a producer of another Pipeline by passing the return value of the Produces
method. A pipeline can also directly consume the result of another Pipeline
with the Consumes method.

Examples of composing pipelines can be found in the example of the Produces
method.

Stopping Short

Sometimes it is desirable to stop a pipeline short of its completion. A good
example would be that if we use a pipeline for a web server, in which case the
producer channel will stay open for as long as the server runs and will not be
closed.

However, we still want a way to force shutdown the server, a switch that if
flicked, would immediately stop the server from accepting new requests and for
the pipeline to organically end.

This is where the context comes in. In the case where the application
needs to stop the pipeline immediately, it should cancel the context, and
the pipeline and all of its stages will stop processing and terminate
gracefully.

Error Handling

A pipeline is designed to fail fast. That is, if any error should occur at any
time at any stage, the pipeline terminates.

The Start function on both Stage and Pipeline returns a channel of errors,
however, as soon as a single error is fed into the channel, the entile pipeline
halts and the error can be read from the channel.

Performance Tuning

There are 3 important dials that would help fine tune the performance
characteristics of a pipeline: the buffer size of the producer, workerPoolSize
and bufferSize of the stage.

Making the producer a buffered channel and adjusting the buffer size allows
any code that sends into the producers to execute without having to block wait
on the pipeline to be ready, which in turn might improve the overall
performance of the application by allowing more code to actually run in
parallel.

Similar to making the producer channel a buffered channel, you can also adjust
the bufferSize of each stage in the pipeline. This is buffer size of the output
channel. This allows the stage to proceed without having to block wait on the
consumer being ready. And similar to the effect of having a buffered producer,
this would potentially allow more code to run in parallel.

The third dial that can affect the performance of a pipeline is the size of the
worker pool. In theory, if the producer can saturate the worker pool, and the
consumer can consume all of the output, then the throughput of each stage
should scale linearly with the size of the worker pool. However,  in reality,
this scaling is affected by many factors and will almost never simply be
linear.

Applications are recommended to run benchmarks with real workloads and tweak
the setting to find the most suitable combination of producer and consumer
buffer sizes and worker pool sizes.

The Benchmark* methods in stage_test.go offers a good starting point on how
to write such benchmark test cases.

*/
package pipeline
