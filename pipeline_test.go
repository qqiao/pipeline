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

	"github.com/qqiao/pipeline"
)

func ExamplePipeline_Consumes() {
	done := make(chan struct{})
	producer := make(chan int)
	consumer := func(out pipeline.Producer[int]) {
		for v := range out {
			fmt.Println(v)
		}
	}

	go func() {
		defer close(producer)
		producer <- 2
	}()

	sq := func(in any) any {
		i := in.(int)
		return i * i
	}
	p := pipeline.NewPipeline[int, int](done)
	p.Consumes(producer)
	p.WithConsumer(consumer)

	_, err := p.AddStage(10, sq)
	if err != nil {
		log.Fatalf("Unable to add stage: %v", err)
	}

	_, err = p.Produces()
	if err != nil {
		log.Fatalf("Unable to run pipeline")
	}

	// Output: 4
}

func ExamplePipeline_Produces() {
	done := make(chan struct{})
	producer := make(chan int)
	consumer := func(out pipeline.Producer[int]) {
		for v := range out {
			fmt.Println(v)
		}
	}

	go func() {
		defer close(producer)
		producer <- 2
	}()

	sq := func(in any) any {
		i := in.(int)
		return i * i
	}
	p, err := pipeline.NewPipelineWithProducer[int, int](done,
		producer).AddStage(10, sq)
	if err != nil {
		log.Fatalf("Unable to add stage: %v", err)
	}

	out, err := p.Produces()
	if err != nil {
		log.Fatalf("Unable to run pipeline")
	}

	consumer(out)

	// Output: 4
}

func ExamplePipeline_Produces_chaining() {
	// In this example, we are going to chain 2 pipelines using the result of
	// the first pipeline's Produces call as the Producer of the second.
	done := make(chan struct{})
	producer := make(chan int)
	consumer := func(out pipeline.Producer[int]) {
		for v := range out {
			fmt.Println(v)
		}
	}

	go func() {
		defer close(producer)
		producer <- 2
	}()

	sq := func(in any) any {
		i := in.(int)
		return i * i
	}
	p1, err := pipeline.NewPipelineWithProducer[int, int](done,
		producer).AddStage(10, sq)
	if err != nil {
		log.Fatalf("Unable to add stage: %v", err)
	}
	p1Producer, err := p1.Produces()
	if err != nil {
		log.Fatalf("Unable to run p1")
	}

	cube := func(in any) any {
		i := in.(int)
		return i * i * i
	}
	// We chain the output channel of p1 into p2 by using it as the producer of
	// p2
	p2, err := pipeline.NewPipelineWithProducer[int, int](done,
		p1Producer).AddStage(10, cube)
	p2.WithConsumer(consumer)

	_, err = p2.Produces()
	if err != nil {
		log.Fatalf("Unable to run pipeline")
	}

	// output: 64
}
