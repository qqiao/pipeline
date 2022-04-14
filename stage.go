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

// Errors for when the stage is in a invalid state
var (
	ErrInvalidBufferSize     = errors.New("invalid buffer size")
	ErrInvalidWorkerPoolSize = errors.New("invalid worker pool size")
)

// Worker represent a unit of work.
//
// Workers are simple functions that takes an input and returns an output. The
// Stage will take care of parallelizing workers and combining the results.
// Since multiple Worker instances will be created, Worker functions are
// expected to be thread-safe to prevent any unpredictable results.
type Worker[I, O any] func(I) O

// Stage represents a single stage in the Pipeline.
//
// Each Stage should be considered a unit of work done to the data in the
// Pipeline.
type Stage[I, O any] struct {
	done           <-chan struct{}
	in             <-chan I
	out            chan O
	workerPoolSize int
	worker         Worker[I, O]
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
	bufferSize int, in Producer[I], worker Worker[I, O]) (*Stage[I, O], error) {
	if workerPoolSize < 1 {
		return nil, ErrInvalidWorkerPoolSize
	}
	if bufferSize < 0 {
		return nil, ErrInvalidBufferSize
	}
	return &Stage[I, O]{
		done:           done,
		in:             in,
		out:            make(chan O, bufferSize),
		workerPoolSize: workerPoolSize,
		worker:         worker,
	}, nil
}

// Produces method starts the processing of the stage, and returns a Producer of
// the output type of the current stage.
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
func (s *Stage[I, O]) Produces() Producer[O] {
	workerIn := make(chan I)
	cs := make(chan (chan O))
	go func() {
		defer close(workerIn)
		defer close(cs)

		workerCount := 0
		for {
			select {
			case input, ok := <-s.in:
				if !ok {
					return
				}
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
			case <-s.done:
				return
			}
		}
	}()
	go s.merge(cs)
	return s.out
}

func (s *Stage[I, O]) merge(cs chan (chan O)) {
	var wg sync.WaitGroup

	output := func(in <-chan O) {
		defer wg.Done()
		for {
			select {
			case o, ok := <-in:
				if !ok {
					return
				}
				s.out <- o
			}
		}
	}

	go func() {
		defer close(s.out)
		for stop := false; !stop; {
			select {
			case c, ok := <-cs:
				if !ok {
					stop = true
				} else {
					wg.Add(1)
					go output(c)
				}
			case <-s.done:
				stop = true
			}

		}
		wg.Wait()
	}()
}
