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

A Producer is a channel from which a Pipeline gets its inputs from, a pipeline
will continue to read from the producer channel until it is closed or anything
is sent into the done channel, more on that later.

A Consumer is any struct that consumes a Producer.

Since a pipeline itself is obviously a consumer, and a pipeline can be
considered a producer since it returns a channel where the results are sent
into, it makes chaining pipelines natural.

A ConsumerFunc, which is a function that takes a channel into which the results
of the Pipeline are sent as its sole argument. Since a channel where values are
sent into is also the definition of a Producer, you can consider a ConsumerFunc
as this:
    type ConsumerFunc[I] func(Producer[I])

More advanced uses of the Consumer and Producer pattern will be discussed
further in the Chaining Pipelines section.

Stages

Stages is heart of the Pipeline. While the API of a stage look extremely
similar to that of a Pipeline, the actual multiplexing of the workers and the
final collation of the results are done by the stage. Therefore the NewStage
function requires additional parameters to control the multiplexing behaviours
of the stage.

Each stage must have a Worker function. A worker is just a simple function that
takes an input and returns an output. The Stage will take care of parallelizing
workers and combining the results. Since multiple Worker instances will be
created, Worker functions are expected to be thread-safe to prevent any
unpredictable results.

The workerPoolSize parameter defines the upper bound of the number of workers.

All Worker goroutines are lazily created. A new worker goroutine is created
when a new input is read from the producer, until workerPoolSize is reached.
Given that goroutines are cheap to create, in order to keep the implementation
as simple as possible, no worker re-use will happen in this growing phase.

Once workerPoolSize is reached, worker goroutines will compete for the inputs
from the producer until the pipeline is done. The pool will not auto shrink for
simplicity reasons.

Chaining Pipelines

A Pipeline also has a set of APIs to make Pipeline chaining straight-forward.
While connecting multiple pipelines will produce identical results as a single
Pipeline with all the constituent stages, there are advantages of having
composable pipelines. The main advantage of making pipelines composable is so
that pipelines from different authors and libraries can be easily re-used.

Without the ability to directly connect pipelines, authors would need to
explicitly create a Stage instance for each stage of their Pipeline, and make
them public, instead of allowing the Pipeline's AddStage API to do it behind
the scenes automatically. More ever, users will also need to ensure that these
stages are added to their own Pipeline in the correct order.

With composable pipelines, this is no longer an issue. A Pipeline can be made
a producer of another Pipeline with the AsProducer method. Or directly consume
the result of another Pipeline with the AsConsumer method.

Examples of composing pipelines can be found in the example section of both
the AsProducer and AsConsumer section.
*/
package pipeline
