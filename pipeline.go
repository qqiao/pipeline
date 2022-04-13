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

type Consumer[I any] interface {
	Consumes(Producer[I])
}
type ConsumerFunc[I any] func(Producer[I])

type Producer[O any] <-chan O

// Pipeline represents data processing Pipeline.
type Pipeline[I, O any] struct {
	consumer ConsumerFunc[O]
	done     <-chan struct{}
	out      chan O
	producer Producer[I]
	stages   []*Stage[any, any]
}

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

func (p *Pipeline[I, O]) Produces() Producer[O] {
	if len(p.stages) < 1 {
		return nil
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
	return p.out
}
