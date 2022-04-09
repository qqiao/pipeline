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

func ExamplePipeline_Start() {
	done := make(chan struct{})
	producer := make(chan int)
	go func() {
		defer close(producer)
		producer <- 2
	}()

	consumer := func(out <-chan int) {
		for v := range out {
			fmt.Println(v)
		}
	}
	sq := func(in any) any {
		i := in.(int)
		return i * i
	}
	p, err := pipeline.NewPipeline[int, int](done, producer,
		consumer).AddStage(10, sq)
	if err != nil {
		log.Fatalf("Unable to add stage: %v", err)
	}
	p.Start()

	// Output: 4
}
