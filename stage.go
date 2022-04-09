package pipeline

import (
	"errors"
	"sync"
)

var (
	ErrInvalidPoolSize = errors.New("invalid pool size")
)

type Stage[I, O any] struct {
	done     <-chan struct{}
	in       <-chan I
	poolSize int
	worker   func(in I) O
}

func NewStage[I, O any](done <-chan struct{}, poolSize int, in <-chan I,
	worker func(in I) O) (*Stage[I, O], error) {
	if poolSize < 1 {
		return nil, ErrInvalidPoolSize
	}
	return &Stage[I, O]{
		done:     done,
		in:       in,
		poolSize: poolSize,
		worker:   worker,
	}, nil
}

func (s *Stage[I, O]) Start() <-chan O {
	workerIn := make(chan I)
	cs := make(chan (chan O))
	go func() {
		defer close(workerIn)
		defer close(cs)

		workerCount := 0
		for input := range s.in {
			if workerCount < s.poolSize {
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
