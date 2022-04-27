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

package pipeline // import "github.com/qqiao/pipeline"

import (
	"context"
	"errors"
	"sync"
)

// Consumer is an interface that wraps the Consume method.
type Consumer[I any] interface {
	Consumes(Producer[I])
}

// ConsumerFunc is a function that consumes values from the Producer
//
// The Consumes function of a Consumer is a ConsumerFunc.
type ConsumerFunc[I any] func(Producer[I])

// Producer is a channel from which a consumer can read its inputs from.
type Producer[O any] <-chan O

// Pipeline represents data processing Pipeline.
type Pipeline[I, O any] struct {
	consumer ConsumerFunc[O]
	err      chan error
	mutex    sync.Mutex
	out      chan O
	producer Producer[I]
	stages   []*Stage[any, any]
	started  bool
}

// Errors for invalid pipeline states.
var (
	ErrNoProducer             = errors.New("no producer")
	ErrNoStage                = errors.New("no stage")
	ErrModificationAfterStart = errors.New("modification after start")
)

// NewPipeline creates a pipeline.
//
// Please note that pipelines created this way does not have a producer
// channel, thus calling AddStage before calling Consumes will result in
// AddStage throwing an ErrNoProducer.
func NewPipeline[I, O any]() *Pipeline[I, O] {
	return &Pipeline[I, O]{
		err:     make(chan error),
		out:     make(chan O),
		stages:  make([]*Stage[any, any], 0),
		started: false,
	}
}

// NewPipelineWithProducer creates a pipeline with the producer given.
//
// Internally this function simply calls NewPipeline and then the Consumes
// method on the returned pipeline in sequence, this method is exactly
// equivalent to the following code:
//
//     pipeline := NewPipeline[I, O]()
//     pipeline.Consumes(producer)
func NewPipelineWithProducer[I, O any](producer Producer[I]) *Pipeline[I, O] {
	pipeline := NewPipeline[I, O]()
	pipeline.Consumes(producer)
	return pipeline
}

// AddStage adds a stage to the pipeline.
//
// Internally, this method first transforms the worker to a StreamWorker and
// then calls AddStageStreamWorker. So this function is exactly equivalent
// to the following:
//     sw := NewStreamWorker(worker)
//     p.AddStageStreamWorker(workerPoolSize, bufferSize, sw)
//
// Please refer to the documentation of AddStageStreamWorker for more details.
func (p *Pipeline[I, O]) AddStage(workerPoolSize int, bufferSize int,
	worker Worker[any, any]) (*Pipeline[I, O], error) {
	return p.AddStageStreamWorker(workerPoolSize, bufferSize,
		NewStreamWorker(worker))
}

// AddStageStreamWorker adds a stage to the pipeline.
//
// This method first creates a new Stage instance by calling
// NewStageStreamWorker and then adds it to the end of list of stages. Please
// refer to documentations of NewStageStreamWorker for details on the
// parameters.
func (p *Pipeline[I, O]) AddStageStreamWorker(workerPoolSize int,
	bufferSize int, worker StreamWorker[any, any]) (*Pipeline[I, O], error) {
	p.mutex.Lock()

	if p.started {
		return nil, ErrModificationAfterStart
	}
	if p.producer == nil {
		return nil, ErrNoProducer
	}

	var producerFunc func() Producer[any]
	if len(p.stages) == 0 {
		producerFunc = func() Producer[any] {
			out := make(chan any)

			go func() {
				defer close(out)
				for v := range p.producer {
					out <- v
				}
			}()
			return out
		}
	} else {
		producerFunc = p.stages[len(p.stages)-1].Produces
	}

	stage, err := NewStageStreamWorker(workerPoolSize, bufferSize,
		producerFunc(), worker)
	if err != nil {
		return nil, err
	}
	p.stages = append(p.stages, stage)
	p.mutex.Unlock()
	return p, nil
}

// Consumes sets the producer of this pipeline.
func (p *Pipeline[I, O]) Consumes(producer Producer[I]) {
	p.producer = producer
}

// Produces returns a channel into which results of the pipeline will be sent.
//
// This method throws a ErrNoStage if the current pipeline does not have any
// stages.
func (p *Pipeline[I, O]) Produces() (Producer[O], error) {
	if len(p.stages) < 1 {
		return nil, ErrNoStage
	}

	return p.out, nil
}

// Start method starts the processing of the pipeline.
//
// This method returns a channel of errors, however because of the fail-fast
// nature of pipelines. The pipeline will stop execution when it encounters
// the first error.
func (p *Pipeline[I, O]) Start(ctx context.Context) <-chan error {
	p.mutex.Lock()
	if !p.started {
		errIn := make(chan (<-chan error))
		go func() {
			for _, stage := range p.stages {
				errIn <- stage.Start(ctx)
			}
		}()
		go func() {
			defer close(p.out)
			for {
				select {
				case v, ok := <-p.stages[len(p.stages)-1].Produces():
					if !ok {
						return
					}
					p.out <- v.(O)
				case <-ctx.Done():
					return
				}
			}
		}()

		if p.consumer != nil {
			p.consumer(p.out)
		}

		go Merge(ctx, p.err, errIn)
		p.started = true
	}
	p.mutex.Unlock()
	return p.err
}

// WithConsumer sets the consumer of the pipeline.
//
// Please read the package level documentation for different approaches
// of consuming the result of a pipeline and their comparisons.
func (p *Pipeline[I, O]) WithConsumer(consumer ConsumerFunc[O]) *Pipeline[I, O] {
	p.consumer = consumer
	return p
}

// Merge takes a channel of channels, and merges the content that has been sent
// into those separate channels into a single channel.
func Merge[O any](ctx context.Context, out chan<- O, inputs <-chan (<-chan O)) {
	var wg sync.WaitGroup
	defer close(out)

	output := func(in <-chan O) {
		defer wg.Done()
		for {
			select {
			case o, ok := <-in:
				if !ok {
					return
				}
				out <- o
			case <-ctx.Done():
				return
			}
		}
	}
	for stop := false; !stop; {
		select {
		case c, ok := <-inputs:
			if !ok {
				stop = true
			} else {
				wg.Add(1)
				go output(c)
			}
		case <-ctx.Done():
			stop = true
		}

	}
	wg.Wait()
}
