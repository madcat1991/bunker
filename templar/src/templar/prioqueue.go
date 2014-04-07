package main

import (
	"container/heap"
)

type PriorityQueueItem interface {
	Score() int
}

// An IntHeap is a min-heap of ints.
type PriorityQueueInternal []PriorityQueueItem

func (h PriorityQueueInternal) Len() int           { return len(h) }
func (h PriorityQueueInternal) Less(i, j int) bool { return h[i].Score() < h[j].Score() }
func (h PriorityQueueInternal) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *PriorityQueueInternal) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(PriorityQueueItem))
}

func (h *PriorityQueueInternal) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type PriorityQueue struct {
	q *PriorityQueueInternal
}

func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{}
	pq.q = &PriorityQueueInternal{}
	heap.Init(pq.q)
	return pq
}

func (self *PriorityQueue) Push(item PriorityQueueItem) {
	heap.Push(self.q, item)
}

func (self *PriorityQueue) Pop() PriorityQueueItem {
	item := heap.Pop(self.q)
	return item.(PriorityQueueItem)
}

func (self *PriorityQueue) Count() int {
	return len(*self.q)
}

type SampleQueueItem struct {
	score int
	value int
}

func NewSampleQueueItem(score int, value int) *SampleQueueItem {
	item := &SampleQueueItem{}
	item.score = score
	item.value = value
	return item
}

func (self *SampleQueueItem) Score() int {
	return self.score
}
