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

package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/qqiao/pipeline/v2"
)

func sq(in int) (int, error) {
	return in * in, nil
}

func cube(in int) (int, error) {
	return in * in * in, nil
}

func build[I, O any](workerPoolSize int, bufferSize int,
	worker pipeline.Worker[I, O]) (chan I, *pipeline.Stage[I, O]) {
	in := make(chan I)

	stage, err := pipeline.NewStage(workerPoolSize, bufferSize, in, worker)
	if err != nil {
		log.Fatalf("Unable to create stage. Error: %v", err)
	}

	return in, stage
}

func BenchmarkWorkerPoolSize1BufferSize0(b *testing.B) {
	in, stage := build(1, 0, cube)

	go func() {
		for n := 0; n < b.N; n++ {
			in <- n
		}
		close(in)
	}()

	out := stage.Produces()
	stage.Start(context.Background())
	for range out {
	}
}

func BenchmarkWorkerPoolSize10BufferSize0(b *testing.B) {
	in, stage := build(10, 0, cube)

	go func() {
		for n := 0; n < b.N; n++ {
			in <- n
		}
		close(in)
	}()

	out := stage.Produces()
	stage.Start(context.Background())
	for range out {
	}
}

func BenchmarkWorkerPoolSize1BufferSize10(b *testing.B) {
	in, stage := build(1, 10, cube)

	go func() {
		for n := 0; n < b.N; n++ {
			in <- n
		}
		close(in)
	}()

	out := stage.Produces()
	stage.Start(context.Background())
	for range out {
	}
}

func BenchmarkWorkerPoolSize10BufferSize10(b *testing.B) {
	in, stage := build(10, 10, cube)

	go func() {
		for n := 0; n < b.N; n++ {
			in <- n
		}
		close(in)
	}()

	out := stage.Produces()
	stage.Start(context.Background())
	for range out {
	}
}

func ExampleStage_Start() {
	input := make(chan int)

	// sq takes an integer and returns the square of that integer
	sq := func(in int) (int, error) {
		return in * in, nil
	}

	stage, err := pipeline.NewStage(10, 0, input, sq)
	if err != nil {
		log.Panicf("Error creating stage: %v", err)
	}

	output := stage.Produces()
	stage.Start(context.Background())
	input <- 2
	input <- 3
	close(input)

	for v := range output {
		fmt.Println(v)
	}

	// Unordered Output:
	// 4
	// 9
}

func ExampleStage_Start_ordered() {
	type OrderedEntry struct {
		Order int
		Value int
	}
	input := make(chan OrderedEntry)

	// sq takes an integer and returns the square of that integer
	// in this example, it also takes an additional parameter which is the
	// order of the input. This value is also returned as part of the result
	// so that we can ensure that the order of the results matches the order
	// of the input
	sq := func(in OrderedEntry) (OrderedEntry, error) {
		return OrderedEntry{
			Order: in.Order,
			Value: in.Value * in.Value,
		}, nil
	}

	stage, err := pipeline.NewStage(10, 5, input, sq)
	if err != nil {
		log.Panicf("Error creating stage: %v", err)
	}

	output := stage.Produces()
	stage.Start(context.Background())
	input <- OrderedEntry{0, 2}
	input <- OrderedEntry{1, 3}
	close(input)

	var results []OrderedEntry
	for v := range output {
		results = append(results, v)
	}

	// Once we have the results, we sort based on the order
	sort.Slice(results, func(i, j int) bool {
		return results[i].Order < results[j].Order
	})
	fmt.Printf("%v", results)

	// Output: [{0 4} {1 9}]
}

func TestNewStage(t *testing.T) {
	in := make(chan int)

	t.Run("Should throw ErrInvalidBufferSize", func(t *testing.T) {
		t.Parallel()
		if _, err := pipeline.NewStage(10, -1, in,
			sq); err != pipeline.ErrInvalidBufferSize {
			t.Errorf("Expected ErrInvalidBufferSize for bufferSize of -1, got nil")
		}
	})

	t.Run("Should throw ErrInvalidWorkerPoolSize", func(t *testing.T) {
		t.Parallel()
		if _, err := pipeline.NewStage(0, 3, in,
			sq); err != pipeline.ErrInvalidWorkerPoolSize {
			t.Errorf("Expected ErrInvalidWorkerPoolSize for workerPoolSize of 0, got nil")
		}
	})
}

func TestStage_Start(t *testing.T) {
	expected := []int{1, 4, 9, 16, 25,
		36, 49, 64, 81, 100}
	t.Run("Should be able to process more data than worker count",
		func(t *testing.T) {
			t.Parallel()
			input := make(chan int)

			// Marking a stage with only 1 worker
			stage, err := pipeline.NewStage(1, 0, input, sq)
			if err != nil {
				t.Errorf("Error creating stage: %v", err)
			}

			output := stage.Produces()
			stage.Start(context.Background())

			go func() {
				defer close(input)
				for i := 1; i < 11; i++ {
					input <- i
				}
			}()

			var got []int
			for result := range output {
				got = append(got, result)
			}
			sort.Ints(got)
			if !reflect.DeepEqual(expected, got) {
				t.Errorf("Expected: %v.\nGot: %v", expected, got)
			}
		})

	t.Run("Should work consistently with any buffer value", func(t *testing.T) {
		t.Parallel()
		input := make(chan int)

		// Marking a stage with only 1 worker
		stage, err := pipeline.NewStage(1, 8, input, sq)
		if err != nil {
			t.Errorf("Error creating stage: %v", err)
		}

		output := stage.Produces()
		stage.Start(context.Background())

		go func() {
			defer close(input)
			for i := 1; i < 11; i++ {
				input <- i
			}
		}()

		var got []int
		for result := range output {
			got = append(got, result)
		}
		sort.Ints(got)
		if !reflect.DeepEqual(expected, got) {
			t.Errorf("Expected: %v.\nGot: %v", expected, got)
		}
	})

	t.Run("Should terminate early with cancellation", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		in := make(chan int)

		stage, err := pipeline.NewStage(1, 0, in, sq)
		if err != nil {
			t.Errorf("Error creating stage: %v", err)
		}

		output := stage.Produces()
		stage.Start(ctx)

		// We send infinitely many inputs, so if cancel didn't happen, the
		// pipeline won't stop, and the test will timeout and fail
		go func() {
			for {
				in <- 1
			}
		}()

		// If the cancellation did not work above, the test will dead-lock here
		for range output {
		}
	})

	t.Run("Should fail fast on error", func(t *testing.T) {
		t.Parallel()
		in := make(chan int)
		go func() {
			i := 0
			for {
				in <- i
				i++
			}
		}()

		er := errors.New("input is greater than 5")

		fn := func(in int) (int, error) {
			if in > 5 {
				return in, er
			}
			return in, nil
		}
		stage, err := pipeline.NewStage(1, 0, in, fn)
		if err != nil {
			t.Errorf("Error creating stage: %v", err)
		}

		out := stage.Produces()
		errCh := stage.Start(context.Background())

		// If fail fast didn't happen, the following loop will infinite loop
		for done := false; !done; {
			select {
			case <-out:
			case err = <-errCh:
				done = true
			}
		}

		if er != err {
			t.Errorf("Expecting error, but didn't get")
		}
	})

	t.Run("Should be thread-safe and reentrant", func(t *testing.T) {
		t.Parallel()
		input := make(chan int)

		// Marking a stage with only 1 worker
		stage, err := pipeline.NewStage(1, 8, input, sq)
		if err != nil {
			t.Errorf("Error creating stage: %v", err)
		}
		output := stage.Produces()

		ctx := context.Background()

		// multiple different ways of calling the stage.Start
		stage.Start(ctx)
		stage.Start(ctx)
		go func() {
			stage.Start(ctx)
		}()

		go func() {
			defer close(input)
			for i := 1; i < 11; i++ {
				input <- i
			}
		}()

		var got []int
		for result := range output {
			got = append(got, result)
		}
		sort.Ints(got)
		if !reflect.DeepEqual(expected, got) {
			t.Errorf("Expected: %v.\nGot: %v", expected, got)
		}
	})
}
