package iot

import (
	"math"
	"math/rand"
)

type random struct {
	Seed int64
	r    *rand.Rand
}

func (r *random) Init() {
	r.r = rand.New(rand.NewSource(r.Seed))
}

func (r *random) Choice(s []string) string {
	return s[r.r.Intn(len(s))]
}

func (r *random) CurrentLoad(old, max, threshold float64) float64 {
	if r.u(0, 1) > threshold || old == 0 {
		return r.fp(r.u(0, max), 0)
	}
	return old
}

func (r *random) ReadingsField(state, low, high, min, max, precision float64) float64 {
	return r.fp(r.crw(r.u(low, high), min, max, r.r.Float64()*state), precision)
}

func (r *random) FuelState(state, low, high, min, max, precision float64) float64 {
	c := r.crw(r.u(low, high), min, max, state)
	if c == min {
		c = max
	}
	return r.fp(c, precision)
}

// u is uniform
func (r *random) u(low, high float64) float64 {
	x := r.r.Float64()
	x *= high - low
	x += low
	return x
}

// crw is clamped random walk
func (r *random) crw(step, min, max, state float64) float64 {
	state += step
	if state > max {
		return max
	}
	if state < min {
		return min
	}
	return state
}

// fp is float precision
func (r *random) fp(step, precision float64) float64 {
	precision = math.Pow(10, precision)
	return float64(int(step*precision)) / precision
}
