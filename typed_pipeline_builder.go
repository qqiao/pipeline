package pipeline

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// ErrBuilderModificationAfterBuild is returned when an attempt is made to modify
// the pipeline builder after Build() has been called or if an attempt is made to
// further modify a builder instance that has already contributed to a Build() call.
var ErrBuilderModificationAfterBuild = errors.New("pipeline builder cannot be modified after Build() has been called or pipeline started")

// HeterogeneousProducer is a type alias for <-chan any. It is used by the
// TypedPipelineBuilder for internal channel connections between stages whose concrete
// types are different. Type safety around these connections is enforced by the
// TypedPipelineBuilder at the time stages are added and when the pipeline is built.
type HeterogeneousProducer <-chan any

// stageDefinition holds the internal configuration for a single stage within a
// typed pipeline. It stores the type-erased StreamWorker (genericStreamWorker)
// along with its operational parameters like workerPoolSize and bufferSize.
type stageDefinition struct {
	// genericStreamWorker is created by AddTypedStage. It wraps the user's
	// strongly-typed Worker[In, Out] function and handles type assertions
	// to bridge between the generic pipeline stages (which use `any`) and
	// the specific types of the worker.
	genericStreamWorker StreamWorker[any, any]
	workerPoolSize      int // Number of concurrent workers for this stage
	bufferSize          int // Size of the output buffer for this stage
}

// TypedPipelineBuilder facilitates the construction of a multi-stage pipeline
// with compile-time type safety for connections between stages.
// Stages are added sequentially by passing the builder instance returned from
// the previous AddTypedStage call to the next, culminating in a call to Build().
//
// CurrentOut represents the output type of the most recently added stage. This type
// changes as new stages are appended to the builder.
//
// RootInput represents the input type of the very first stage in the pipeline. This
// type is fixed when the TypedPipelineBuilder is created via NewTypedPipeline.
type TypedPipelineBuilder[CurrentOut any, RootInput any] struct {
	stages  []stageDefinition // Stores the definition of each stage in the pipeline
	mutex   sync.Mutex        // Protects access to the stages slice and started flag
	started bool              // True if the pipeline has been built or started, preventing further modifications
}

// NewTypedPipeline creates and returns a new TypedPipelineBuilder, serving as the
// entry point for constructing a type-safe pipeline.
//
// InputType specifies the input type of the first stage that will be added to this
// pipeline, which also defines the RootInput type for the entire pipeline.
// Initially, as no stages have been added, CurrentOut is also set to InputType.
func NewTypedPipeline[InputType any]() *TypedPipelineBuilder[InputType, InputType] {
	return &TypedPipelineBuilder[InputType, InputType]{
		stages:  make([]stageDefinition, 0),
		started: false,
	}
}

// AddTypedStage is a standalone generic function that adds a new, strongly-typed
// processing stage to a pipeline configuration. It takes an existing TypedPipelineBuilder
// instance and returns a new one, reflecting the addition of the new stage and
// the change in the pipeline's current output type. This design allows for type-safe,
// sequential construction of pipelines, e.g.:
//   b0 := NewTypedPipeline[int]()
//   b1, _ := AddTypedStage(b0, workerPoolSize, bufferSize, intToStringWorker)
//   b2, _ := AddTypedStage(b1, workerPoolSize, bufferSize, stringToStructWorker)
//   pipeline, _ := b2.Build()
//
// It ensures type safety between the output of the previous stage (CurrentOut)
// and the input of the newly added stage.
//
// Type Parameters:
//   - CurrentOut: The output type of the last stage in the input 'builder'.
//   - NextOutType: The output type of the worker function for the stage being added.
//   - RootInput: The input type of the very first stage in the pipeline.
//
// Parameters:
//   - builder: The TypedPipelineBuilder instance to which this stage is logically appended.
//   - workerPoolSize: The number of concurrent workers for this new stage.
//   - bufferSize: The buffer size for the output channel of this new stage.
//   - typedWorker: The strongly-typed function (Worker[CurrentOut, NextOutType]) that processes data for this stage.
func AddTypedStage[CurrentOut any, NextOutType any, RootInput any](
	builder *TypedPipelineBuilder[CurrentOut, RootInput],
	workerPoolSize int,
	bufferSize int,
	typedWorker Worker[CurrentOut, NextOutType],
) (*TypedPipelineBuilder[NextOutType, RootInput], error) {
	builder.mutex.Lock() // Lock the original builder while reading its state
	defer builder.mutex.Unlock()

	if builder.started {
		return nil, ErrBuilderModificationAfterBuild
	}

	if workerPoolSize < 1 {
		return nil, ErrInvalidWorkerPoolSize
	}
	if bufferSize < 0 {
		return nil, ErrInvalidBufferSize
	}

	// Define gswInner with the non-hanging signature (input <-chan any)
	gswInner := func(ctx context.Context, input <-chan any) (<-chan any, <-chan error) {
		outputChan := make(chan any, bufferSize)
		errorChan := make(chan error, 1)

		go func() {
			defer close(outputChan)
			defer close(errorChan)

			workerCtx, cancelWorker := context.WithCancel(ctx)
			defer cancelWorker()

			for val := range input {
				typedVal, ok := val.(CurrentOut)
				if !ok {
					var expectedType CurrentOut
					errorChan <- fmt.Errorf("type assertion failed in pipeline stage: expected type %T, but received type %T for value %v", expectedType, val, val)
					return
				}

				result, err := typedWorker(typedVal)
				if err != nil {
					errorChan <- err
					return
				}

				select {
				case outputChan <- result:
				case <-workerCtx.Done():
					errorChan <- workerCtx.Err()
					return
				}
			}
		}()
		return outputChan, errorChan
	}

	// Define gswOuter wrapper with the exact signature of StreamWorker[any, any]
	// (using Producer[any] for input, and its underlying types for output)
	gswOuter := func(ctx context.Context, pInput Producer[any]) (<-chan any, <-chan error) {
		// pInput is Producer[any], which is <-chan any.
		// gswInner expects <-chan any for its second argument.
		// The return types also match.
		return gswInner(ctx, pInput)
	}

	newStageDef := stageDefinition{
		genericStreamWorker: gswOuter, // Assign the outer wrapper
		workerPoolSize:      workerPoolSize,
		bufferSize:          bufferSize,
	}

	// Create a new list of stages for the new builder
	newStagesList := make([]stageDefinition, len(builder.stages)+1)
	copy(newStagesList, builder.stages)
	newStagesList[len(builder.stages)] = newStageDef

	// Return a new builder instance
	return &TypedPipelineBuilder[NextOutType, RootInput]{
		stages: newStagesList,
		// mutex will be a new zero-value mutex for this new struct instance
		// started will be false for this new struct instance
	}, nil
}

// Build finalizes the pipeline construction process using the stages added via
// AddTypedStage. It assembles these stages into a runnable *pipeline.Pipeline instance.
//
// The resulting pipeline will have an input type matching the RootInput of the
// builder and an output type matching the FinalOutput (which is the CurrentOut
// type parameter of the builder instance upon which Build is called).
//
// After Build is called on a builder instance, that instance is marked as "started"
// and cannot be further modified (e.g., by calling AddTypedStage or Build again).
// AddTypedStage itself returns new builder instances, promoting a more immutable
// style of pipeline definition.
//
// The returned *pipeline.Pipeline requires its input producer to be set via its
// Consumes() method before it can be started.
//
// Possible errors include:
//   - ErrNoStage: if Build is called before any stages are added.
//   - ErrBuilderModificationAfterBuild: if Build is called on an already used builder.
//   - Errors from adding stages to the underlying pipeline structure if parameters
//     were somehow invalid at that level (though AddTypedStage aims to catch these early).
func (p *TypedPipelineBuilder[FinalOutput, RootInput]) Build() (*Pipeline[RootInput, FinalOutput], error) {
	p.mutex.Lock()
	if p.started {
		p.mutex.Unlock()
		return nil, ErrBuilderModificationAfterBuild
	}
	p.started = true
	p.mutex.Unlock()

	if len(p.stages) == 0 {
		return nil, ErrNoStage
	}

	pipe := NewPipeline[RootInput, FinalOutput]()

	var err error
	for _, stageDef := range p.stages {
		_, err = pipe.AddStageStreamWorker(stageDef.workerPoolSize,
			stageDef.bufferSize,
			stageDef.genericStreamWorker,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to add stage to pipeline during build: %w", err)
		}
	}

	return pipe, nil
}
