package pipeline

type Producer[P any] interface {
	Produce() <-chan P
}
