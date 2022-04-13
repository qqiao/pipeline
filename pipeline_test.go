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
	go func() {
		defer close(producer)
		producer <- 2
	}()

	consumer := func(out pipeline.Producer[int]) {
		for v := range out {
			fmt.Println(v)
		}
	}
	sq := func(in any) any {
		i := in.(int)
		return i * i
	}
	p := pipeline.NewPipeline[int](done, consumer)
	p.Consumes(producer)
	_, err := p.AddStage(10, sq)
	if err != nil {
		log.Fatalf("Unable to add stage: %v", err)
	}
	p.Produces()

	// Output: 4
}

func ExamplePipeline_Produces() {
	done := make(chan struct{})
	producer := make(chan int)
	go func() {
		defer close(producer)
		producer <- 2
	}()

	consumer := func(out pipeline.Producer[int]) {
		for v := range out {
			fmt.Println(v)
		}
	}
	sq := func(in any) any {
		i := in.(int)
		return i * i
	}
	p, err := pipeline.NewPipelineWithProducer(done, consumer,
		producer).AddStage(10, sq)
	if err != nil {
		log.Fatalf("Unable to add stage: %v", err)
	}
	p.Produces()

	// Output: 4
}
