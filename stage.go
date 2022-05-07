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
	"sync"
)

// Errors for when the stage is in an invalid state
var (
	ErrInvalidBufferSize     = errors.New("invalid buffer size")
	ErrInvalidWorkerPoolSize = errors.New("invalid worker pool size")
)

// Stage represents a single stage in the Pipeline.
//
// Each Stage should be considered a unit of work done to the data in the
// Pipeline.
type Stage[I, O any] struct {
	err            chan error
	in             <-chan I
	mutex          sync.Mutex
	out            chan O
	started        bool
	workerPoolSize int
	worker         StreamWorker[I, O]
}

// NewStage creates a new stage with necessary parameters specified.
//
// Internally, this function is nothing but a shorthand for the following:
//     sw := NewStreamWorker(worker)
//     stage := NewStageStreamWorker(workerPoolSize, bufferSize, in, sw)
//
// Please refer to documentation of NewStageStreamWorker for details of all
// parameters.
func NewStage[I, O any](workerPoolSize int, bufferSize int, in Producer[I],
	worker Worker[I, O]) (*Stage[I, O], error) {
	return NewStageStreamWorker(workerPoolSize, bufferSize, in,
		NewStreamWorker(worker))
}

// NewStageStreamWorker creates a new stage with the given StreamWorker and
// parameters.
//
// Parameters
//
// workerPoolSize: internally, the stage maintains a pool of workers all
// running concurrently, workerPoolSize specifies the upper bound of the
// possible number of workers.
//
// bufferSize: size of the output buffer. Setting a bufferSize of greater than
// 0 will make the output channel a buffered channel, which will allow some
// work to be done concurrently without having to block wait for the consumer.
//
// in: channel where input will be read from
//
// worker: is the function that actually processes each unit of data read from
// the in channel.
//
// This function returns ErrInvalidBufferSize if bufferSize is less than 0.
//
// This function returns ErrInvalidWorkerPoolSize if workerPoolSize is not at
// least 1.
func NewStageStreamWorker[I, O any](workerPoolSize int, bufferSize int,
	in Producer[I], worker StreamWorker[I, O]) (*Stage[I, O], error) {
	if workerPoolSize < 1 {
		return nil, ErrInvalidWorkerPoolSize
	}
	if bufferSize < 0 {
		return nil, ErrInvalidBufferSize
	}

	return &Stage[I, O]{
		err:            make(chan error),
		in:             in,
		out:            make(chan O, bufferSize),
		started:        false,
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
	s.mutex.Lock()
	if !s.started {
		cs := make(chan (<-chan O))
		es := make(chan (<-chan error))

		go func() {
			defer close(cs)
			defer close(es)

			for i := 0; i < s.workerPoolSize; i++ {
				c, e := s.worker(ctx, s.in)
				cs <- c
				es <- e
			}
		}()
		go Merge(ctx, s.out, cs)
		go Merge(ctx, s.err, es)
		s.started = true
	}
	s.mutex.Unlock()
	return s.err
}
