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

package pipeline

import "errors"

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
	done     <-chan struct{}
	out      chan O
	producer Producer[I]
	stages   []*Stage[any, any]
}

// Errors for invalid pipeline states
var (
	ErrNoProducer = errors.New("no producer")
	ErrNoStage    = errors.New("no stage")
)

// NewPipeline creates a pipeline with the done channel and consumer function
// given.
//
// Please note that pipelines created this way does not have a producer channel,
// thus calling AddStage before calling Consumes will result in AddStage
// throwing an ErrNoProducer.
func NewPipeline[I, O any](done <-chan struct{}, consumer ConsumerFunc[O]) *Pipeline[I, O] {
	return &Pipeline[I, O]{
		consumer: consumer,
		done:     done,
		out:      make(chan O),
		stages:   make([]*Stage[any, any], 0),
	}
}

func NewPipelineWithProducer[I, O any](done <-chan struct{},
	consumer ConsumerFunc[O], producer Producer[I]) *Pipeline[I, O] {
	pipeline := NewPipeline[I](done, consumer)
	pipeline.Consumes(producer)

	return pipeline
}

func (p *Pipeline[I, O]) AddStage(workerPoolSize int,
	worker Worker[any, any]) (*Pipeline[I, O], error) {
	if p.producer == nil {
		return nil, ErrNoProducer
	}
	var producerFunc func() Producer[any]
	if len(p.stages) == 0 {
		producerFunc = func() Producer[any] {
			out := make(chan any)

			go func() {
				defer close(out)
				for {
					select {
					case v, ok := <-p.producer:
						if !ok {
							return
						}
						out <- v
					case <-p.done:
						return
					}
				}
			}()

			return out
		}
	} else {
		producerFunc = p.stages[len(p.stages)-1].Start
	}

	stage, err := NewStage(p.done, workerPoolSize, producerFunc(), worker)
	if err != nil {
		return nil, err
	}

	p.stages = append(p.stages, stage)
	return p, nil
}

func (p *Pipeline[I, O]) Consumes(producer Producer[I]) {
	p.producer = producer
}

func (p *Pipeline[I, O]) Produces() (Producer[O], error) {
	if len(p.stages) < 1 {
		return nil, ErrNoStage
	}

	var stageOut <-chan any
	for _, stage := range p.stages {
		stageOut = stage.Start()
	}

	go func() {
		defer close(p.out)
		for {
			select {
			case v, ok := <-stageOut:
				if !ok {
					return
				}
				p.out <- v.(O)
			case <-p.done:
				return
			}
		}
	}()

	p.consumer(p.out)
	return p.out, nil
}
