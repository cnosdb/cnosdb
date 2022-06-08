package iot

type Case struct {
	Query  string
	Result Results
}

type Suite struct {
	Gen Generator
}
