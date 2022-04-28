package pipeline

import "context"

// StreamWorker is a type of worker that continuously takes input from the
// producer and continuously produces output into the output channel.
//
// A StreamWorker also returns an error channel, any time anything is sent
// into this channel, the stage and pipeline will immediately halt the
// execution. This is to ensure that the pipeline stays consistent with the
// fail-fast characteristics.
//
// StreamWorkers will be multiplexed based on workerPoolSize. Thus, they need
// to be thread safe.
type StreamWorker[I, O any] func(context.Context, Producer[I]) (<-chan O,
	<-chan error)

// Worker represent a unit of work.
//
// Workers are simple functions that takes an input and returns an output. The
// Stage will take care of the parallelization of workers and the combination
// of the results.
//
// Since multiple Worker instances will be created, Worker functions are
// expected to be thread-safe to prevent any unpredictable results.
//
// Any time an error is returned, the stage and pipeline will immediately halt
// any execution. This is to stay consistent with the fail-fast
// characteristics.
//
// Due to the fact that Workers might be multiplexed, they should be
// thread-safe.
type Worker[I, O any] func(I) (O, error)

// NewStreamWorker takes any Worker and converts it into a StreamWorker.
//
// This allows the worker to be reused. Under the surface, the stage is simply
// multiplexing StreamWorkers based on workerPoolSize.
func NewStreamWorker[I, O any](worker Worker[I, O]) StreamWorker[I, O] {
	return func(ctx context.Context, in Producer[I]) (<-chan O, <-chan error) {
		output := make(chan O)
		errCh := make(chan error)

		_ctx, cancel := context.WithCancel(ctx)
		go func() {
			defer close(output)
			defer close(errCh)
			for {
				select {
				case i, ok := <-in:
					if !ok {
						return
					}
					out, err := worker(i)
					if err != nil {
						errCh <- err
						cancel()
						return
					}
					output <- out

				case <-_ctx.Done():
					return
				}
			}
		}()
		return output, errCh
	}
}
