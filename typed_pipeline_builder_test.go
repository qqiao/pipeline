package pipeline_test

import (
	"errors"
	"testing"

	"github.com/qqiao/pipeline/v2" // Adjust import path if necessary for your environment
)

// dummyIntToStringWorker is a placeholder worker for tests that don't depend on worker logic.
func dummyIntToStringWorker(i int) (string, error) {
	return "dummy", nil
}

// dummyStringToStringWorker is a placeholder for testing AddTypedStage with a string input.
func dummyStringToStringWorker(s string) (string, error) {
	return s, nil
}

// TestTypedPipelineBuilder_Build_Empty tests that calling Build() on a builder
// with no stages returns ErrNoStage.
func TestTypedPipelineBuilder_Build_Empty(t *testing.T) {
	t.Parallel()
	builder := pipeline.NewTypedPipeline[int]()
	_, err := builder.Build()

	if !errors.Is(err, pipeline.ErrNoStage) {
		t.Errorf("Expected ErrNoStage, got %v", err)
	}
}

// TestTypedPipelineBuilder_AddTypedStage_AfterBuild tests that attempting to add a stage
// to a builder instance *after* Build() has been called on that same instance
// results in ErrBuilderModificationAfterBuild.
func TestTypedPipelineBuilder_AddTypedStage_AfterBuild(t *testing.T) {
	t.Parallel()

	// Create the initial builder instance.
	initialBuilder := pipeline.NewTypedPipeline[int]()

	// Add one stage. AddTypedStage returns a new builder instance.
	// builderAfterStageAdd is the instance that has the stage and on which Build() will be called.
	builderAfterStageAdd, err := pipeline.AddTypedStage(initialBuilder, 1, 0, dummyIntToStringWorker)
	if err != nil {
		t.Fatalf("Initial AddTypedStage failed unexpectedly: %v", err)
	}

	// Call Build() on the builder instance that has the stage.
	// This marks builderAfterStageAdd as 'started'.
	_, err = builderAfterStageAdd.Build() // This call sets builderAfterStageAdd.started = true
	if err != nil {
		t.Fatalf("Build() failed unexpectedly: %v", err)
	}

	// Now, try to call AddTypedStage again, using 'builderAfterStageAdd' as the input builder.
	// builderAfterStageAdd has CurrentOut = string. We need a worker that accepts string.
	// This attempt should fail because builderAfterStageAdd.started is true.
	_, err = pipeline.AddTypedStage(builderAfterStageAdd, 1, 0, dummyStringToStringWorker)
	if !errors.Is(err, pipeline.ErrBuilderModificationAfterBuild) {
		t.Errorf("Expected ErrBuilderModificationAfterBuild when calling AddTypedStage with a builder that has already been built, got %v", err)
	}

	// For completeness, also check the initialBuilder instance.
	// Calling AddTypedStage on initialBuilder should still work as it was not the instance
	// on which Build() was called (unless AddTypedStage modified initialBuilder in place,
	// which it shouldn't for a fluent interface returning new instances).
	// The current implementation of AddTypedStage modifies p.mutex and p.started of the *receiver*.
	// This means initialBuilder.started is still false.
	// However, the returned builder from the first AddTypedStage is a *new* struct.
	// Let's clarify the behavior being tested: it's the instance *on which Build was called*.
	// The builder returned by AddTypedStage is a new struct, but the p.started flag is on the instance.
	// The initial AddTypedStage call was on 'initialBuilder'. It returned 'builderAfterStageAdd'.
	// 'builderAfterStageAdd' is a new struct.
	// The Build call was on 'builderAfterStageAdd'. So 'builderAfterStageAdd.started' is true.
	// The p.started flag on 'initialBuilder' itself should remain false.
	// The test as written above correctly tests 'builderAfterStageAdd'.
}

// TestTypedPipelineBuilder_AddTypedStage_InputValidation tests input validation
// for the AddTypedStage method.
func TestTypedPipelineBuilder_AddTypedStage_InputValidation(t *testing.T) {
	t.Parallel()

	t.Run("Invalid workerPoolSize", func(t *testing.T) {
		t.Parallel()
		builder := pipeline.NewTypedPipeline[int]()
		// Call AddTypedStage as a standalone function
		_, err := pipeline.AddTypedStage(builder, 0, 0, dummyIntToStringWorker) // workerPoolSize = 0
		if !errors.Is(err, pipeline.ErrInvalidWorkerPoolSize) {
			t.Errorf("Expected ErrInvalidWorkerPoolSize for workerPoolSize=0, got %v", err)
		}
	})

	t.Run("Invalid bufferSize", func(t *testing.T) {
		t.Parallel()
		builder := pipeline.NewTypedPipeline[int]()
		// Call AddTypedStage as a standalone function
		_, err := pipeline.AddTypedStage(builder, 1, -1, dummyIntToStringWorker) // bufferSize = -1
		if !errors.Is(err, pipeline.ErrInvalidBufferSize) {
			t.Errorf("Expected ErrInvalidBufferSize for bufferSize=-1, got %v", err)
		}
	})
}
