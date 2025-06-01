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
	"reflect" // Added reflect
	"strconv" // Added strconv
	"testing"
	"time"

	"github.com/qqiao/pipeline/v2"
)

// MyStruct is used in TestTypedPipeline_BasicTwoStage
type MyStruct struct {
	Value string
}

func TestTypedPipeline_BasicTwoStage(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Define typed workers
	intToStringWorker := func(i int) (string, error) {
		return fmt.Sprintf("val:%d", i), nil
	}
	stringToMyStructWorker := func(s string) (MyStruct, error) {
		return MyStruct{Value: s}, nil
	}

	// Build the typed pipeline
	builder0 := pipeline.NewTypedPipeline[int]()
	builder1, err := pipeline.AddTypedStage(builder0, 1, 0, intToStringWorker)
	if err != nil {
		t.Fatalf("Failed to add intToStringWorker stage: %v", err)
	}
	builder2, err := pipeline.AddTypedStage(builder1, 1, 0, stringToMyStructWorker)
	if err != nil {
		t.Fatalf("Failed to add stringToMyStructWorker stage: %v", err)
	}
	typedPipeline, err := builder2.Build()

	if err != nil {
		t.Fatalf("Failed to build typed pipeline: %v", err)
	}

	// Setup input producer
	inputStream := make(chan int, 2) // Buffered to prevent sender blocking before Start
	typedPipeline.Consumes(inputStream)

	// Get output consumer
	outputStream, err := typedPipeline.Produces()
	if err != nil {
		t.Fatalf("Failed to get pipeline output channel: %v", err)
	}

	// Start the pipeline
	errChan := typedPipeline.Start(ctx)

	// Feed input
	go func() {
		defer close(inputStream)
		inputStream <- 10
		inputStream <- 20
	}()

	// Collect results
	var results []MyStruct
	var pipelineErr error

	outputDone := false
	errorDone := false
	for !outputDone || !errorDone {
		select {
		case res, ok := <-outputStream:
			if !ok {
				outputDone = true
				continue
			}
			results = append(results, res)
		case errVal, ok := <-errChan:
			if !ok {
				errorDone = true
				continue
			}
			if pipelineErr == nil { // Store first error
				pipelineErr = errVal
			}
		}
	}

	if pipelineErr != nil {
		t.Errorf("Pipeline reported an unexpected error: %v", pipelineErr)
	}

	expectedResults := []MyStruct{
		{Value: "val:10"},
		{Value: "val:20"},
	}

	// With workerPoolSize = 1 for all stages, the order should be preserved.
	if !reflect.DeepEqual(results, expectedResults) {
		t.Errorf("Expected results %v, got %v", expectedResults, results)
	}
}

func TestTypedPipeline_WorkerError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Define typed worker that can error
	stringToFloatWorker := func(s string) (float64, error) {
		if s == "error" {
			return 0, errors.New("worker error triggered")
		}
		val, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return 0, fmt.Errorf("strconv error: %w", err)
		}
		return val, nil
	}

	// Build the typed pipeline
	builder0 := pipeline.NewTypedPipeline[string]()
	builder1, err := pipeline.AddTypedStage(builder0, 1, 0, stringToFloatWorker)
	if err != nil {
		t.Fatalf("Failed to add stringToFloatWorker stage: %v", err)
	}
	typedPipeline, err := builder1.Build()

	if err != nil {
		t.Fatalf("Failed to build typed pipeline: %v", err)
	}

	// Setup input producer
	inputStream := make(chan string, 3) // Buffered
	typedPipeline.Consumes(inputStream)

	// Get output consumer
	outputStream, err := typedPipeline.Produces()
	if err != nil {
		t.Fatalf("Failed to get pipeline output channel: %v", err)
	}

	// Start the pipeline
	errChan := typedPipeline.Start(ctx)

	// Feed input
	go func() {
		defer close(inputStream)
		inputStream <- "1.23"
		inputStream <- "error" // This will cause an error
		inputStream <- "4.56"  // This might not be processed
	}()

	// Collect results and error
	var results []float64
	var firstPipelineError error
	var allErrors []error

	outputClosed := false
	pipelineErrorClosed := false

	for !outputClosed || !pipelineErrorClosed {
		select {
		case res, ok := <-outputStream:
			if !ok {
				outputClosed = true
				continue
			}
			results = append(results, res)
		case errVal, ok := <-errChan:
			if !ok {
				pipelineErrorClosed = true
				continue
			}
			allErrors = append(allErrors, errVal)
			if firstPipelineError == nil {
				firstPipelineError = errVal
			}
		}
	}

	// Check for the specific worker error
	if firstPipelineError == nil {
		t.Errorf("Expected a pipeline error, but got none. All errors: %v", allErrors)
	} else {
		expectedErrorMsg := "worker error triggered"
		// The error from AddStageStreamWorker in pipeline.go or the genericStreamWorker wrapper
		// might wrap the original error. We should check using errors.Is or string contains.
		// Our genericStreamWorker sends the error directly.
		// The pipeline's Start method merges errors, so it should be the direct error.
		if firstPipelineError.Error() != expectedErrorMsg {
			t.Errorf("Expected error message '%s', got '%s'", expectedErrorMsg, firstPipelineError.Error())
		}
	}

	// Check that not all inputs were processed if the error occurred mid-stream.
	// With fail-fast, after "error", "4.56" should not be processed into a float.
	// We expect only 1.23 to be in results.
	expectedResults := []float64{1.23}
	if !reflect.DeepEqual(results, expectedResults) {
		t.Errorf("Expected results %v due to error, got %v", expectedResults, results)
	}
}

func ExamplePipeline_Consumes() {
	producer := make(chan int)
	consumer := func(out pipeline.Producer[int]) {
		for v := range out {
			fmt.Println(v)
		}
	}

	go func() {
		defer close(producer)
		producer <- 2
		producer <- 3
	}()

	sq := func(in any) (any, error) {
		i := in.(int)
		return i * i, nil
	}
	p := pipeline.NewPipeline[int, int]()
	p.Consumes(producer)
	p.WithConsumer(consumer)

	_, err := p.AddStage(10, 0, sq)
	if err != nil {
		log.Fatalf("Unable to add stage: %v", err)
	}
	p.Start(context.Background())
	if err != nil {
		log.Fatalf("Unable to run pipeline")
	}

	// Unordered Output:
	// 4
	// 9
}

func ExamplePipeline_Produces_chaining() {
	// In this example, we are going to chain 2 pipelines using the result of
	// the first pipeline's Produces call as the Producer of the second.
	ctx := context.Background()
	producer := make(chan int)
	consumer := func(out pipeline.Producer[int]) {
		for v := range out {
			fmt.Println(v)
		}
	}

	go func() {
		defer close(producer)
		producer <- 2
		producer <- 3
	}()

	sq := func(in any) (any, error) {
		i := in.(int)
		return i * i, nil
	}
	p1, err := pipeline.NewPipelineWithProducer[int, int](producer).AddStage(10, 0, sq)
	if err != nil {
		log.Fatalf("Unable to add stage: %v", err)
	}
	p1Producer, err := p1.Produces()
	if err != nil {
		log.Fatalf("Unable to get pipeline1's producer")
	}

	cube := func(in any) (any, error) {
		i := in.(int)
		return i * i * i, nil
	}
	// We chain the output channel of p1 into p2 by using it as the producer of
	// p2
	p2, err := pipeline.NewPipelineWithProducer[int, int](p1Producer).AddStage(10, 0, cube)
	if err != nil {
		log.Fatalf("Unable to create pipeline 2")
	}
	p2.WithConsumer(consumer)
	p1.Start(ctx)
	p2.Start(ctx)

	// Unordered Output:
	// 64
	// 729
}

func ExamplePipeline_Start() {
	producer := make(chan int)
	consumer := func(out pipeline.Producer[int]) {
		for v := range out {
			fmt.Println(v)
		}
	}

	go func() {
		defer close(producer)
		producer <- 2
		producer <- 3
	}()

	sq := func(in any) (any, error) {
		i := in.(int)
		return i * i, nil
	}
	p, err := pipeline.NewPipelineWithProducer[int, int](producer).AddStage(10, 0, sq)
	if err != nil {
		log.Fatalf("Unable to add stage: %v", err)
	}

	out, err := p.Produces()
	p.Start(context.Background())
	if err != nil {
		log.Fatalf("Unable to run pipeline")
	}

	consumer(out)

	// Unordered Output:
	// 4
	// 9
}

func ExamplePipeline_Start_stopping() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	producer := make(chan int)

	p, err := pipeline.NewPipelineWithProducer[int,
		int](producer).AddStage(1, 0, func(in any) (any, error) {
		return in, nil
	})
	if err != nil {
		log.Fatalf("Unable to create pipeline: %v", err)
	}

	// Simulates an infinite input
	go func() {
		for {
			producer <- 1
		}
	}()

	out, err := p.Produces()
	if err != nil {
		log.Fatalf("Unable to execute pipeline: %v", err)
	}
	p.Start(ctx)

	// This part would infinite loop if we didn't cancel the context
	for range out {
	}

	// Output:
}

func TestPipeline_AddStage(t *testing.T) {
	t.Run("Should return error when no producer is present", func(t *testing.T) {
		t.Parallel()
		p := pipeline.NewPipeline[int, int]()
		if _, err := p.AddStage(1, 0, func(input any) (any, error) {
			return input, nil
		}); err == nil {
			t.Error("Expecting ErrNoProducer, got nil")
		}
	})
}

func TestPipeline_AddStageStreamWorker(t *testing.T) {
	t.Run("Should ErrModificationAfterStart on modification after start",
		func(t *testing.T) {
			t.Parallel()
			in := make(chan int)
			go func() {
				i := 0
				for {
					in <- i
					i++
				}
			}()

			addOne := func(input any) (any, error) {
				in := input.(int)
				return in + 1, nil
			}

			p := pipeline.NewPipelineWithProducer[int, int](in)
			if _, err := p.AddStageStreamWorker(1, 0,
				pipeline.NewStreamWorker(addOne)); err != nil {
				t.Errorf("Failed to add first stage: %v", err)
			}
			ch, err := p.Produces()
			if err != nil {
				t.Errorf("Error getting producer of pipeline: %v", err)
			}

			go func() {
				for range ch {
				}
			}()

			// starting the pipeline
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			p.Start(ctx)

			if _, err = p.AddStageStreamWorker(1, 0,
				pipeline.NewStreamWorker(addOne)); err != pipeline.
				ErrModificationAfterStart {
				t.Errorf("Expecting ErrModificationAfterStart")
			}
		})

	t.Run("Should be thread-safe", func(t *testing.T) {
		t.Parallel()
		in := make(chan int)

		addOne := func(input any) (any, error) {
			in := input.(int)
			return in + 1, nil
		}

		p := pipeline.NewPipelineWithProducer[int, int](in)
		if _, err := p.AddStageStreamWorker(1, 0,
			pipeline.NewStreamWorker(addOne)); err != nil {
			t.Errorf("Failed to add first stage: %v", err)
		}

		go func() {
			for i := 0; i < 20; i++ {
				p.AddStageStreamWorker(1, 0, pipeline.NewStreamWorker(addOne))
			}
		}()

		go func() {
			for i := 0; i < 20; i++ {
				p.AddStageStreamWorker(1, 0, pipeline.NewStreamWorker(addOne))
			}
		}()

		// Any data race issue, the race detector should detect this
	})
}

func TestPipeline_Start_failFast(t *testing.T) {
	er := errors.New("input greater than 5")
	in := make(chan int)
	go func() {
		i := 0
		for {
			in <- i
			i++
		}
	}()

	p := pipeline.NewPipelineWithProducer[int, int](in)
	if _, err := p.AddStage(1, 0, func(input any) (any, error) {
		in := input.(int)
		return in + 1, nil
	}); err != nil {
		t.Errorf("Failed to add first stage: %v", err)
	}

	if _, err := p.AddStage(1, 0, func(input any) (any, error) {
		in := input.(int)
		if in > 5 {
			return in, er
		}
		return in * in, nil
	}); err != nil {
		t.Error("Failed to add second stage")
	}

	if _, err := p.AddStage(1, 0, func(input any) (any, error) {
		in := input.(int)
		return in + 1, nil
	}); err != nil {
		t.Error("Failed to add third stage")
	}

	out, _ := p.Produces()
	errCh := p.Start(context.Background())

	var err error

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
}
