package main

import (
	"testing"
)

func TestPriorityQueue(t *testing.T) {
	q := NewPriorityQueue()
	q.Push(NewSampleQueueItem(5, 2))
	q.Push(NewSampleQueueItem(3, 1))
	q.Push(NewSampleQueueItem(8, 3))

	v1 := q.Pop().(*SampleQueueItem)
	v2 := q.Pop().(*SampleQueueItem)
	v3 := q.Pop().(*SampleQueueItem)
	if v1.value != 1 {
		t.FailNow()
	}
	if v2.value != 2 {
		t.FailNow()
	}
	if v3.value != 3 {
		t.FailNow()
	}
	if q.Count() != 0 {
		t.FailNow()
	}
}

func TestIntSet(t *testing.T) {
	s := NewIntSet([]int{})
	s.Add(4)
	s.Add(5)
	s.Add(8)
	s.Add(4)
	if !s.Contains(4) {
		t.FailNow()
	}
	if !s.Contains(5) {
		t.FailNow()
	}
	if !s.Contains(8) {
		t.FailNow()
	}

	s_a := s.AsArray()
	if len(s_a) != 3 {
		t.FailNow()
	}

	s1 := NewIntSet(s_a)
	if !s1.Contains(4) {
		t.FailNow()
	}
	if !s1.Contains(5) {
		t.FailNow()
	}
	if !s1.Contains(8) {
		t.FailNow()
	}

	s2 := NewIntSet([]int{4,9,1,8})
	v := s1.WeightedIntersect(s2, nil)
	if v != 2.0 {
		t.Fatalf("%f", v)
	}

	s1.Update([]int{13, 9})
	v = s1.WeightedIntersect(s2, nil)
	if v != 3.0 {
		t.Fatalf("%f", v)
	}

	w1 := map[int]float64{
		9: 2.0,
		13: 4.0,
	}
	v = s1.WeightedIntersect(s2, w1)
	if v != 4.0 {
		t.Fatalf("%f", v)
	}
}
