package examples

import (
	"context"
	"fmt"
	"strconv"
	// Assuming your pipeline package is "github.com/qqiao/pipeline/v2"
	// This import path should match how the package is resolved in the testing/example environment.
	"github.com/qqiao/pipeline/v2"
)

// CustomData is a sample struct for the ExampleTypedPipelineBuilder_multiStage example.
type CustomData struct {
	ID      int
	Message string
}

// ExampleTypedPipelineBuilder_multiStage demonstrates how to use the TypedPipelineBuilder
// to create a multi-stage pipeline with type safety for intermediate stages.
func ExampleTypedPipelineBuilder_multiStage() {
	// Define worker functions with specific types
	intToString := func(i int) (string, error) {
		return fmt.Sprintf("ID-%d", i), nil
	}

	stringToCustomData := func(s string) (CustomData, error) {
		// Example: parse string like "ID-123"
		if len(s) < 4 { // Basic check for "ID-" prefix
			return CustomData{}, fmt.Errorf("invalid string format: %s", s)
		}
		numPart := s[3:] // "123"
		id, err := strconv.Atoi(numPart)
		if err != nil {
			return CustomData{}, fmt.Errorf("failed to parse ID from %s: %w", numPart, err)
		}
		return CustomData{ID: id, Message: s}, nil
	}

	// 1. Create a new typed pipeline builder starting with int input.
	// Note: In a real _test package, you might use pipeline.NewTypedPipeline if examples are part of it.
	// For a separate examples package, ensure the pipeline package is correctly imported.
	builder := pipeline.NewTypedPipeline[int]()

	// 2. Add stages with type-safe workers.
	// Stage 1: int -> string
	builder1, err := pipeline.AddTypedStage(builder, 1, 0, intToString)
	if err != nil {
		fmt.Printf("Error adding intToString stage: %v\n", err)
		return
	}
	// Stage 2: string -> CustomData
	builder2, err := pipeline.AddTypedStage(builder1, 1, 0, stringToCustomData)
	if err != nil {
		fmt.Printf("Error adding stringToCustomData stage: %v\n", err)
		return
	}
	// Final pipeline is Pipeline[int, CustomData]
	builtPipeline, err := builder2.Build()

	if err != nil {
		fmt.Printf("Error building pipeline: %v\n", err)
		return
	}

	// 3. Set up the input producer for the pipeline.
	inputChan := make(chan int, 2) // Buffered to allow sending before start
	builtPipeline.Consumes(inputChan)

	// 4. Define a consumer for the pipeline's output.
	var results []CustomData
	outputConsumer := func(out pipeline.Producer[CustomData]) {
		for data := range out {
			results = append(results, data)
		}
	}
	builtPipeline.WithConsumer(outputConsumer)

	// 5. Start the pipeline.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure context is cancelled to free resources

	errChan := builtPipeline.Start(ctx)

	// 6. Send data to the input channel.
	//    This needs to be in a goroutine if the pipeline might block
	//    on input processing or if inputChan is unbuffered.
	//    Since inputChan is buffered and WithConsumer runs in its own goroutine after Start,
	//    this can be sequential for a small number of inputs if Start is non-blocking for setup.
	//    However, standard practice is to use a goroutine for feeding input.
	go func() {
		defer close(inputChan)
		inputChan <- 101
		inputChan <- 202
	}()

	// 7. Wait for the pipeline to complete (or handle errors).
	//    The error channel closes when the pipeline finishes processing all data
	//    (i.e., input channel closed and all items processed) or when an
	//    unrecoverable error occurs and the context is cancelled.
	pipelineError, isOpen := <-errChan
	if isOpen && pipelineError != nil { // Check if error channel was open and an error was sent
		fmt.Printf("Pipeline execution error: %v\n", pipelineError)
	}

	// Print results. With workerPoolSize=1 in all stages, order is preserved.
	for _, r := range results {
		fmt.Printf("ID: %d, Message: %s\n", r.ID, r.Message)
	}

	// Output:
	// ID: 101, Message: ID-101
	// ID: 202, Message: ID-202
}
