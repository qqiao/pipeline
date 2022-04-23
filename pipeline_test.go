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
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/qqiao/pipeline"
)

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

	sq := func(in any) any {
		i := in.(int)
		return i * i
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

func ExamplePipeline_Produces() {
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

	sq := func(in any) any {
		i := in.(int)
		return i * i
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

	sq := func(in any) any {
		i := in.(int)
		return i * i
	}
	p1, err := pipeline.NewPipelineWithProducer[int, int](producer).AddStage(10, 0, sq)
	if err != nil {
		log.Fatalf("Unable to add stage: %v", err)
	}
	p1Producer, err := p1.Produces()
	p1.Start(ctx)
	if err != nil {
		log.Fatalf("Unable to run p1")
	}

	cube := func(in any) any {
		i := in.(int)
		return i * i * i
	}
	// We chain the output channel of p1 into p2 by using it as the producer of
	// p2
	p2, err := pipeline.NewPipelineWithProducer[int, int](p1Producer).AddStage(10, 0, cube)
	p2.WithConsumer(consumer)
	p2.Start(ctx)
	if err != nil {
		log.Fatalf("Unable to run pipeline")
	}

	// Unordered Output:
	// 64
	// 729
}

func ExamplePipeline_Produces_stoppingShort() {
	ctx, cancel := context.WithCancel(context.Background())
	producer := make(chan int)

	p, err := pipeline.NewPipelineWithProducer[int, int](producer).AddStage(1, 0, func(in any) any {
		return in
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

	// Without this goroutine, the pipeline will simply run on forever
	// However, by cancelling the context in 2 seconds, we demonstrate that
	// it will stop the pipeline
	go func() {
		select {
		case <-time.After(2 * time.Second):
			cancel()
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
		p := pipeline.NewPipeline[int, int]()
		if _, err := p.AddStage(1, 0, func(input any) any {
			return input
		}); err == nil {
			t.Error("Expecting ErrNoProducer, got nil")
		}
	})
}
