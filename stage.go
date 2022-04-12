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

import (
	"errors"
	"sync"
)

var (
	ErrInvalidWorkerPoolSize = errors.New("invalid worker pool size")
)

type Worker[I, O any] func(I) O

// Stage represents a single stage in the pipeline.
//
// Each Stage should be considered a unit of work done to the data in the
// pipeline.
type Stage[I, O any] struct {
	done           <-chan struct{}
	in             <-chan I
	workerPoolSize int
	worker         func(I) O
}

// NewStage creates a new stage with necessary parameters specified.
//
// done: done is a ready only channel where if any value is sent into, the
// stage immediately stops.
//
// workerPoolSize: internally, the stage maintains a pool of workers all
// running in parallel, workerPoolSize specifies the upper bound of the
// possible number of workers.
//
// in: channel where input will be read from
//
// worker: is the function that actually processes each unit of data read from
// the in channel.
//
// This function returns ErrInvalidWorkerPoolSize if workerPoolSize is not at
// least 1.
func NewStage[I, O any](done <-chan struct{}, workerPoolSize int,
	in Producer[I], worker Worker[I, O]) (*Stage[I, O], error) {
	if workerPoolSize < 1 {
		return nil, ErrInvalidWorkerPoolSize
	}
	return &Stage[I, O]{
		done:           done,
		in:             in,
		workerPoolSize: workerPoolSize,
		worker:         worker,
	}, nil
}

// Start method starts the processing of the stage.
//
// This method is responsible for creating the worker pool, distributing work
// between the workers and collating the results in the end.
//
// Workers are created lazily by this method. That is, for a stage with
// workerPoolSize of n, the first n inputs each start a worker.
//
// The worker pool also does not shrink, that is, once n workers are created,
// they will keep on serving until either the stage is explicitly terminated
// by sending into the done channel, or the in channel is closed.
//
// Please also note that order is NOT guaranteed by the Stage. That is, results
// could come out of the channel in different order from they were read in the
// input channel.
func (s *Stage[I, O]) Start() <-chan O {
	workerIn := make(chan I)
	cs := make(chan (chan O))
	go func() {
		defer close(workerIn)
		defer close(cs)

		workerCount := 0
		for input := range s.in {
			if workerCount < s.workerPoolSize {
				workerOut := make(chan O)
				go func() {
					defer close(workerOut)
					for {
						select {
						case i, ok := <-workerIn:
							if !ok {
								return
							}
							workerOut <- s.worker(i)
						case <-s.done:
							return
						}
					}
				}()

				cs <- workerOut
				workerCount++
			}

			workerIn <- input
		}
	}()

	return s.merge(s.done, cs)
}

func (s *Stage[I, O]) merge(done <-chan struct{}, cs chan (chan O)) <-chan O {
	var wg sync.WaitGroup
	out := make(chan O)

	output := func(in <-chan O) {
		defer wg.Done()
		for o := range in {
			select {
			case out <- o:
			case <-done:
				return
			}
		}
	}

	go func() {
		defer close(out)
		for c := range cs {
			wg.Add(1)
			go output(c)
		}
		wg.Wait()
	}()

	return out
}
