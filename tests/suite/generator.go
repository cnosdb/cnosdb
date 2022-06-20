package suite

type Generator interface {
	Init()
	Run()
	Parallel() int
}
