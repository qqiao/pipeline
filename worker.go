package pipeline

import "context"

// StreamWorker is a type of worker that continuously takes input from the
// producer and continuously produces output into the output channel.
//
// A StreamWorker also returns an error channel, any time anything is sent
// into this channel, the execution of the entire pipeline will halt
// immediately. This is to ensure that the pipeline correctly maintains its
// fail-fast characteristics.
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
type Worker[I, O any] func(I) (O, error)

// NewStreamWorker takes any Worker and converts it into a StreamWorker
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
