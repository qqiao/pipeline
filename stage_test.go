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

	stage, err := pipeline.NewStage[int, int](done, 10, input, func(in int) int {
		return in * in
	})
	if err != nil {
		log.Panicf("Error creating stage: %v", err)
	}

	output := stage.Start()
	input <- 3
	close(input)

	var results []int
	for v := range output {
		results = append(results, v)
	}
	fmt.Printf("%v", results)

	// Output: [9]
}

func TestStage_Start(t *testing.T) {
	expected := []int{1, 4, 9, 16, 25,
		36, 49, 64, 81, 100}
	t.Run("Should be able to process more data than worker count",
		func(t *testing.T) {
			done := make(chan struct{})
			input := make(chan int)

			// Marking a stage with only 1 worker
			stage, err := pipeline.NewStage[int, int](done, 1, input, func(in int) int {
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
