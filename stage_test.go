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
	"fmt"
	"log"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/qqiao/pipeline"
)

func build[I, O any](workerPoolSize int, bufferSize int,
	worker pipeline.Worker[I, O]) (chan struct{}, chan I, *pipeline.Stage[I, O]) {
	done := make(chan struct{})
	in := make(chan I)

	stage, err := pipeline.NewStage(done, workerPoolSize, bufferSize, in, worker)
	if err != nil {
		log.Fatalf("Unable to create stage. Error: %v", err)
		os.Exit(1)
	}

	return done, in, stage
}

func BenchmarkWorkerPoolSize1BufferSize0(b *testing.B) {
	_, in, stage := build(1, 0, func(in int) int {
		return in * in * in
	})

	go func() {
		for n := 0; n < b.N; n++ {
			in <- n
		}
		close(in)
	}()

	out := stage.Produces()
	for range out {
	}
}

func BenchmarkWorkerPoolSize10BufferSize0(b *testing.B) {
	_, in, stage := build(10, 0, func(in int) int {
		return in * in * in
	})

	go func() {
		for n := 0; n < b.N; n++ {
			in <- n
		}
		close(in)
	}()

	out := stage.Produces()
	for range out {
	}
}

func BenchmarkWorkerPoolSize1BufferSize10(b *testing.B) {
	_, in, stage := build(1, 10, func(in int) int {
		return in * in * in
	})

	go func() {
		for n := 0; n < b.N; n++ {
			in <- n
		}
		close(in)
	}()

	out := stage.Produces()
	for range out {
	}
}

func BenchmarkWorkerPoolSize10BufferSize10(b *testing.B) {
	_, in, stage := build(10, 10, func(in int) int {
		return in * in * in
	})

	go func() {
		for n := 0; n < b.N; n++ {
			in <- n
		}
		close(in)
	}()

	out := stage.Produces()
	for range out {
	}
}

func ExampleStage_Produces() {
	done := make(chan struct{})
	input := make(chan int)

	// sq takes an integer and returns the square of that integer
	sq := func(in int) int {
		return in * in
	}

	stage, err := pipeline.NewStage(done, 10, 0, input, sq)
	if err != nil {
		log.Panicf("Error creating stage: %v", err)
	}

	output := stage.Produces()
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

func ExampleStage_Produces_ordered() {
	type OrderedEntry struct {
		Order int
		Value int
	}
	done := make(chan struct{})
	input := make(chan OrderedEntry)

	// sq takes an integer and returns the square of that integer
	// in this example, it also takes an additional parameter which is the
	// order of the input. This value is also returned as part of the result
	// so that we can ensure that the order of the results matches the order
	// of the input
	sq := func(in OrderedEntry) OrderedEntry {
		return OrderedEntry{
			Order: in.Order,
			Value: in.Value * in.Value,
		}
	}

	stage, err := pipeline.NewStage(done, 10, 5, input, sq)
	if err != nil {
		log.Panicf("Error creating stage: %v", err)
	}

	output := stage.Produces()
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
	done := make(chan struct{})
	in := make(chan int)
	worker := func(in int) int {
		return in
	}

	t.Run("Should throw ErrInvalidBufferSize", func(t *testing.T) {
		t.Parallel()
		if _, err := pipeline.NewStage(done, 10, -1, in, worker); err != pipeline.ErrInvalidBufferSize {
			t.Errorf("Expected ErrInvalidBufferSize for bufferSize of -1, got nil")
		}
	})

	t.Run("Should throw ErrInvalidWorkerPoolSize", func(t *testing.T) {
		t.Parallel()
		if _, err := pipeline.NewStage(done, 0, 3, in, worker); err != pipeline.ErrInvalidWorkerPoolSize {
			t.Errorf("Expected ErrInvalidWorkerPoolSize for workerPoolSize of 0, got nil")
		}
	})
}

func TestStage_Produces(t *testing.T) {
	expected := []int{1, 4, 9, 16, 25,
		36, 49, 64, 81, 100}
	t.Run("Should be able to process more data than worker count",
		func(t *testing.T) {
			t.Parallel()
			done := make(chan struct{})
			input := make(chan int)

			// Marking a stage with only 1 worker
			stage, err := pipeline.NewStage(done, 1, 0, input, func(in int) int {
				return in * in
			})
			if err != nil {
				log.Panicf("Error creating stage: %v", err)
			}

			output := stage.Produces()

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
		done := make(chan struct{})
		input := make(chan int)

		// Marking a stage with only 1 worker
		stage, err := pipeline.NewStage(done, 1, 8, input, func(in int) int {
			return in * in
		})
		if err != nil {
			log.Panicf("Error creating stage: %v", err)
		}

		output := stage.Produces()

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

	t.Run("Should terminate early when done is closed", func(t *testing.T) {
		t.Parallel()
		done := make(chan struct{})
		in := make(chan int)

		stage, err := pipeline.NewStage(done, 1, 0, in, func(in int) int {
			return in * in
		})
		if err != nil {
			log.Panicf("Error creating stage: %v", err)
		}

		output := stage.Produces()

		// We send infinitely many inputs, so if done isn't closed, the
		// pipeline won't stop, and the test will timeout and fail
		go func() {
			for {
				in <- 1
			}
		}()

		// After 2 seconds, we close the done channel, which would terminate
		// the pipeline
		go func() {
			select {
			case <-time.After(2 * time.Second):
				close(done)
			}
		}()

		// If the close(done) did not work above, the test will dead-lock here
		for range output {
		}
	})
}
