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

type Pipeline[I, O any] struct {
	consumer func(<-chan O)
	done     <-chan struct{}
	producer <-chan I
	stages   []*Stage[any, any]
}

func NewPipeline[I, O any](done <-chan struct{},
	producer <-chan I, consumer func(<-chan O)) *Pipeline[I, O] {
	return &Pipeline[I, O]{
		consumer: consumer,
		done:     done,
		producer: producer,
		stages:   make([]*Stage[any, any], 0),
	}
}

func (p *Pipeline[I, O]) AddStage(workerPoolSize int,
	worker func(any) any) (*Pipeline[I, O], error) {
	var producer func() <-chan any
	if len(p.stages) == 0 {
		producer = func() <-chan any {
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
		producer = p.stages[len(p.stages)-1].Start
	}

	stage, err := NewStage[any, any](p.done, workerPoolSize, producer(), worker)
	if err != nil {
		return nil, err
	}

	p.stages = append(p.stages, stage)
	return p, nil
}

func (p *Pipeline[I, O]) Start() {
	if len(p.stages) < 1 {
		return
	}

	out := make(chan O)

	var stageOut <-chan any
	for _, stage := range p.stages {
		stageOut = stage.Start()
	}

	go func() {
		defer close(out)
		for {
			select {
			case v, ok := <-stageOut:
				if !ok {
					return
				}
				out <- v.(O)
			case <-p.done:
				return
			}
		}
	}()

	p.consumer(out)
}
