package main

import (
	"github.com/mapzen/neatlacoche/OSMPBF"
	"fmt"
)

// Sorter handles sorting nodes, ways and relations into one or many grid
// squares in a concurrent fashion.
type Sorter struct {
	// Quit channels for each of the workers
	workers []chan bool

	// Channels to receive back the results of the worker computation; a map of
	// the item IDs to their grid square(s).
	results []chan chan *MultiBlock

	// Channel of workers which are ready to start work.
	workQueue chan chan *OSMPBF.PrimitiveBlock

	// Last seen "kind" of data; nodes, ways or relations. Because each type can
	// reference the previous one, these need to be done in order.
	lastKind int

	// The global maps of item IDs to their grids. Once a kind has been completed,
	// a read-only copy of the whole data structure is kept here and referenced by
	// later kind computations.
	Nodes, Ways *MultiBlock

	// Number of processes to run.
	numProcs int

	// Range in X & Y coordinates to use for the grid.
	xRange, yRange [2]float64
}

// NewSorter sets up a new Sorter and starts its worker goroutines.
func NewSorter(numProcs int, xRange, yRange [2]float64) (*Sorter, error) {
	s := new(Sorter)
	s.workQueue = make(chan chan *OSMPBF.PrimitiveBlock)
	s.numProcs = numProcs
	s.xRange = xRange
	s.yRange = yRange
	s.lastKind = PKIND_NODE

	s.startNodesWorkers()

	return s, nil
}

// Close cleans up the worker goroutines associated with this Sorter.
func (s *Sorter) Close() {
	for _, ch := range s.workers {
		ch <- true
	}
	close(s.workQueue)
}

const (
	PKIND_NODE = iota
	PKIND_WAY = iota
	PKIND_REL = iota
)

var PKIND_NAMES = [...]string{"node", "way", "relation"}

// primitiveBlockKind returns the "kind" of data inside a PBF primitive block.
// While it's technically possible to mix nodes, ways and relations in a
// primitive block, the PBF reader should ensure that we are given a different
// block for each. This allows us to stop at a block boundary to collect the
// results of the previous kind computation.
func primitiveBlockKind(p *OSMPBF.PrimitiveBlock) int {
	nodes, ways, rels := primCount(p)

	if nodes > 0 {
		if ways > 0 || rels > 0 {
			panic(fmt.Sprintf("Block has %d nodes, but also %d ways and %d relations. Can only handle blocks containing a single type.", nodes, ways, rels))
		}
		return PKIND_NODE

	} else if ways > 0 {
		if rels > 0 {
			panic(fmt.Sprintf("Block has %d ways, but also %d relations. Can only handle blocks containing a single type.", ways, rels))
		}
		return PKIND_WAY

	} else {
		return PKIND_REL
	}
}

// collect results from a kind computation and merge together to make a single,
// global (and constant) map which will be referenced in later computations.
// Also shuts down the workers associated with the current kind.
func (s *Sorter) collect(mb *MultiBlock) {
	// send a ping to all workers to collect results
	ch := make(chan *MultiBlock)
	for i, r := range s.results {
		r <- ch
		rmb := <-ch
		mb.Merge(rmb)
		s.workers[i] <- true
	}
	s.results = nil
	s.workers = nil
}

func (s *Sorter) startNodesWorkers() {
	for i := 0; i < s.numProcs; i += 1 {
		quitChan := make(chan bool)
		resultChan := make(chan chan *MultiBlock)
		go nodeWorkerLoop(s.workQueue, quitChan, i, s.xRange, s.yRange, resultChan)
		s.workers = append(s.workers, quitChan)
		s.results = append(s.results, resultChan)
	}
}

func (s *Sorter) startWaysWorkers(nodes *MultiBlock) {
	for i := 0; i < s.numProcs; i += 1 {
		quitChan := make(chan bool)
		resultChan := make(chan chan *MultiBlock)
		go wayWorkerLoop(s.workQueue, quitChan, i, resultChan, nodes)
		s.workers = append(s.workers, quitChan)
		s.results = append(s.results, resultChan)
	}
}

// Appends a block to the Sorter, sending it to an appropriate worker for
// computation.
func (s *Sorter) Append(p *OSMPBF.PrimitiveBlock) error {
	kind := primitiveBlockKind(p)

	if kind != s.lastKind {
		if kind < s.lastKind {
			return fmt.Errorf("Block kind %q cannot follow kind %d, they must occur in order.", PKIND_NAMES[kind], PKIND_NAMES[s.lastKind])
		}

		if (s.lastKind == PKIND_NODE) {
			s.Nodes = NewMultiBlock()
			s.collect(s.Nodes)
		}
		if (kind == PKIND_WAY) {
			s.startWaysWorkers(s.Nodes)
		}
		if (s.lastKind == PKIND_WAY) {
			s.Ways = NewMultiBlock()
			s.collect(s.Ways)
			// TODO collect extra nodes as well
		}
		// start up new workers
		s.lastKind = kind
	}

	// TODO: handle relations, currently we ignore them
	if kind != PKIND_REL {
		req := <-s.workQueue
		req <- p
	}

	return nil
}
