package model

import (
	"strconv"
	"sync"
	"testing"
)

func TestTrackerConcurrency(t *testing.T) {
	tracker := NewTracker()
	var wg sync.WaitGroup
	ops := 100

	for i := 0; i < ops; i++ {
		wg.Add(1)
		go func() {
			tracker.Store(strconv.Itoa(i), i)
			wg.Done()
		}()
	}

	wg.Wait()

	for i := 0; i < ops; i++ {
		wg.Add(1)
		go func() {
			wg.Done()
			_, ok := tracker.Get(strconv.Itoa(i))
			if !ok {
				t.Errorf("value %v not present", i)
			}
		}()
	}
	wg.Wait()
}
