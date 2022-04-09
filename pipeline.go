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
	consumer Consumer[O]
	done     <-chan any
	producer Producer[I]
	stages   []Stage[any, any]
}

func NewPipeline[I, O any](done <-chan any) *Pipeline[I, O] {
	return &Pipeline[I, O]{
		done:   done,
		stages: make([]Stage[any, any], 0),
	}
}

func (p *Pipeline[I, O]) AddStage(stage Stage[any, any]) *Pipeline[I, O] {
	p.stages = append(p.stages, stage)
	return p
}

func (p *Pipeline[I, O]) WithConsumer(consumer Consumer[O]) *Pipeline[I, O] {
	p.consumer = consumer
	return p
}

func (p *Pipeline[I, O]) WithProducer(producer Producer[I]) *Pipeline[I,
	O] {
	p.producer = producer
	return p
}
