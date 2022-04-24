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

func ExamplePipeline_Start_stoppingShort() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	producer := make(chan int)

	p, err := pipeline.NewPipelineWithProducer[int, int](producer).AddStage(1, 0, func(in any) (any, error) {
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
		p := pipeline.NewPipeline[int, int]()
		if _, err := p.AddStage(1, 0, func(input any) (any, error) {
			return input, nil
		}); err == nil {
			t.Error("Expecting ErrNoProducer, got nil")
		}
	})
}

func TestPipeline_Start_failfast(t *testing.T) {
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
	for cont := true; cont; {
		select {
		case <-out:
		case err = <-errCh:
			cont = false
		}
	}

	if er != err {
		t.Errorf("Expecting error, but didn't get")
	}
}
