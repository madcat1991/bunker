package main

type IntSet struct {
	items map[int]int
	h uint64
}

func NewIntSet(items []int) *IntSet {
	set := &IntSet{}
	set.items = make(map[int]int, len(items))
	for _, i := range(items) {
		set.items[i] = 1;
	}
	return set
}

func (self *IntSet) AsArray() []int {
	result := make([]int, 0, len(self.items))
	for k := range(self.items) {
		result = append(result, k)
	}
	return result
}

func (self *IntSet) Contains(x int) bool {
	_, ok := self.items[x]
	return ok
}

func (self *IntSet) Add(x int) {
	self.items[x] = 1
}

func (self *IntSet) Update(xs []int) {
	for _, x := range(xs) {
		self.Add(x)
	}
}

func (self *IntSet) MaybeIntersects(other *IntSet) bool {
	return true
}

func (self *IntSet) WeightedIntersect(other *IntSet, weights map[int]float64) float64 {
	var total float64 = 0.0
	if !self.MaybeIntersects(other) {
		return 0.0
	}
	for k := range(self.items) {
		_, ok := other.items[k]
		if !ok {
			continue
		}
		w, ok := weights[k]
		if !ok {
			total += 1.0
		} else {
			total += w
		}
	}
	return total
}
