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
	"context"
	"errors"
)

// Errors for when the stage is in an invalid state
var (
	ErrInvalidBufferSize     = errors.New("invalid buffer size")
	ErrInvalidWorkerPoolSize = errors.New("invalid worker pool size")
)

// Worker represent a unit of work.
//
// Workers are simple functions that takes an input and returns an output. The
// Stage will take care of the parallelization of workers and the combination
// of the results.
//
// Since multiple Worker instances will be created, Worker functions are
// expected to be thread-safe to prevent any unpredictable results.
type Worker[I, O any] func(I) (O, error)

// Stage represents a single stage in the Pipeline.
//
// Each Stage should be considered a unit of work done to the data in the
// Pipeline.
type Stage[I, O any] struct {
	in             <-chan I
	out            chan O
	workerPoolSize int
	worker         Worker[I, O]
}

// NewStage creates a new stage with necessary parameters specified.
//
// workerPoolSize: internally, the stage maintains a pool of workers all
// running in parallel, workerPoolSize specifies the upper bound of the
// possible number of workers.
//
// bufferSize: size of the output buffer. Setting a bufferSize of greater than
// 0 will make the output channel a buffered channel, which will allow some
// work to be done in parallel without having to block wait for the consumer.
//
// in: channel where input will be read from
//
// worker: is the function that actually processes each unit of data read from
// the in channel.
//
// This function returns ErrInvalidWorkerPoolSize if workerPoolSize is not at
// least 1.
func NewStage[I, O any](workerPoolSize int, bufferSize int, in Producer[I],
	worker Worker[I, O]) (*Stage[I, O], error) {
	if workerPoolSize < 1 {
		return nil, ErrInvalidWorkerPoolSize
	}
	if bufferSize < 0 {
		return nil, ErrInvalidBufferSize
	}
	return &Stage[I, O]{
		in:             in,
		out:            make(chan O, bufferSize),
		workerPoolSize: workerPoolSize,
		worker:         worker,
	}, nil
}

// Produces returns a Producer where the results of this stage will be sent
// into.
func (s *Stage[I, O]) Produces() Producer[O] {
	return s.out
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
// by cancelling the context, or the in channel is closed.
//
// Please also note that order is NOT guaranteed by the Stage. That is, results
// could come out of the channel in different order from they were read in the
// input channel.
//
// This method also returns a channel of errors, however, due to the fail-fast
// nature of a pipeline, the execution will stop on the first error occurrence.
func (s *Stage[I, O]) Start(ctx context.Context) <-chan error {
	workerIn := make(chan I)
	cs := make(chan (<-chan O))
	errCh := make(chan error)

	// We create a sub-context here so that if an error should occur,
	// we can use this to terminate the program flow.
	_ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(workerIn)
		defer close(cs)
		defer close(errCh)

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
								o, err := s.worker(i)
								if err != nil {
									errCh <- err
									cancel()
									return
								}
								workerOut <- o
							case <-_ctx.Done():
								return
							}
						}
					}()

					cs <- workerOut
					workerCount++
				}

				workerIn <- input
			case <-_ctx.Done():
				return
			}
		}
	}()
	go merge(_ctx, s.out, cs)
	return errCh
}
