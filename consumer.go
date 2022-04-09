package pipeline

type Consumer[P any] interface {
	Consume(<-chan P)
}
