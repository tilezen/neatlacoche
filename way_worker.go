package main

import (
	"github.com/mapzen/neatlacoche/OSMPBF"
	"fmt"
)

type wayWorker struct {
	Ways *MultiBlock
	ExtraNodes map[int64]uint32
	Id int
	Nodes *MultiBlock
}

func wayWorkerLoop(workQueue chan chan *OSMPBF.PrimitiveBlock, quitChan chan bool, i int, resultChan chan chan *MultiBlock, nodes *MultiBlock) {
	w := &wayWorker{
		Ways: NewMultiBlock(),
		ExtraNodes: map[int64]uint32{},
		Id: i,
		Nodes: nodes,
	}
	requestQueue := make(chan *OSMPBF.PrimitiveBlock)

	for {
		select {
		case workQueue <- requestQueue:
		case ch := <-resultChan:
			fmt.Printf("way_worker[%d]: %d\n", i, len(w.ExtraNodes))
			ch <- w.Ways

		case <-quitChan:
			return
		}

		select {
		case work := <-requestQueue:
			w.processWayRequest(work)

		case ch := <-resultChan:
			fmt.Printf("way_worker[%d]: %d\n", i, len(w.ExtraNodes))
			ch <- w.Ways

		case <-quitChan:
			return
		}
	}
}

func (w *wayWorker) processWayRequest(b *OSMPBF.PrimitiveBlock) {
	for _, g := range b.Primitivegroup {
		for _, way := range g.Ways {
			w.putWay(way.Id, way.Refs)
		}
	}
}

func (w *wayWorker) putWay(id int64, nds []int64) {
	mask := uint32(0)
	nd_masks := make([]uint32, len(nds))

	for i, n := range nds {
		nd_masks[i] = w.Nodes.Lookup(n)
		mask = mask | nd_masks[i]
	}

	w.Ways.Append(id, mask)

	for i, n := range nds {
		nd_mask := nd_masks[i]
		if nd_mask != mask {
			w.ExtraNodes[n] = w.ExtraNodes[n] | (mask & ^nd_mask)
		}
	}
}
