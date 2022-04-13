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
	"reflect"
	"sort"
	"testing"

	"github.com/qqiao/pipeline"
)

func ExampleStage_Start() {
	done := make(chan struct{})
	input := make(chan int)

	// sq takes an integer and returns the square of that integer
	sq := func(in int) int {
		return in * in
	}

	stage, err := pipeline.NewStage(done, 10, input, sq)
	if err != nil {
		log.Panicf("Error creating stage: %v", err)
	}

	output := stage.Start()
	input <- 2
	input <- 3
	close(input)

	var results []int
	for v := range output {
		results = append(results, v)
	}
	sort.Ints(results)
	fmt.Printf("%v", results)

	// Output: [4 9]
}

func ExampleStage_Start_ordered() {
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

	stage, err := pipeline.NewStage(done, 10, input, sq)
	if err != nil {
		log.Panicf("Error creating stage: %v", err)
	}

	output := stage.Start()
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

func TestStage_Start(t *testing.T) {
	expected := []int{1, 4, 9, 16, 25,
		36, 49, 64, 81, 100}
	t.Run("Should be able to process more data than worker count",
		func(t *testing.T) {
			done := make(chan struct{})
			input := make(chan int)

			// Marking a stage with only 1 worker
			stage, err := pipeline.NewStage(done, 1, input, func(in int) int {
				return in * in
			})
			if err != nil {
				log.Panicf("Error creating stage: %v", err)
			}

			output := stage.Start()

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
}
