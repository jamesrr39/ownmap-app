package semaphore

import (
	"sync"
)

type Semaphore struct {
	c  chan struct{}
	wg *sync.WaitGroup
}

func NewSemaphore(maxConcurrentOps uint) *Semaphore {
	return &Semaphore{make(chan struct{}, maxConcurrentOps), new(sync.WaitGroup)}
}

func (s *Semaphore) Add() {
	s.wg.Add(1)
	s.c <- struct{}{}
}

func (s *Semaphore) Done() {
	<-s.c
	s.wg.Done()
}

func (s *Semaphore) Wait() {
	s.wg.Wait()
}

func (s *Semaphore) CurrentlyRunning() int {
	return len(s.c)
}
