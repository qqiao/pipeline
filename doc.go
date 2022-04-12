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
Package pipeline contains an implementation of the pipeline concurrency pattern
with Go 1.18 generics support.

Background

This pipeline concurrency pattern is inspired by the blog post from the Go dev
team here: https://go.dev/blog/pipelines.

This library takes the overall idea of the blog post and made it easier to
use by implementing it with support of Generics introduced in Go 1.18.

Additional care has also been taken in the design of the API to ensure that both
connecting multiple pipelines and creating multi-stage pipelines are as easy
as possible.

Basics

A Pipeline consists of the following essential components:

A Producer, which is a channel from which a pipeline gets its inputs from.

A Consumer, which is a function where takes a channel from which the results of
the pipeline are sent into.

A done channel, which, if any value is sent into, terminates the pipeline
gracefully.

A set of Stages, which we will explain in the follow section.

A pipeline also has a set of APIs to make pipeline chaining straight-forward.
While connecting multiple pipelines will produce identical results as a single
pipeline with all the constituent stages, there are advantages of having
composable pipelines. The main advantage of making pipelines composable is so
that pipelines from different authors and libraries can be easily re-used.

Without the ability to directly connect pipelines, authors would need to
explicitly create a Stage instance for each stage of their pipeline, and make
them public, instead of allowing the pipeline's AddStage API to do it behind
the scenes automatically. More ever, users will also need to ensure that these
stages are added to their own pipeline in the correct order.

With composable pipelines, this is no longer an issue. A pipeline can be made
a producer of another pipeline with the AsProducer method. Or directly consume
the result of another pipeline with the AsConsumer method.

Examples of composing pipelines can be found in the example section of both
the AsProducer and AsConsumer section.

Stages

Stages is heart of the pipeline. While the API of a stage look extremely
similar to that of a pipeline, the actual multiplex of the worker and the
final collation of the results are done by the stage. Therefore the NewStage
function contains additional parameters to control the multiplexing behaviours
of the stage.

The workerPoolSize parameter defines the upper bound of the number of workers.
*/
package pipeline
