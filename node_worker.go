package main

import (
	"github.com/mapzen/neatlacoche/OSMPBF"
	"github.com/paulmach/go.geo"
	"fmt"
)

type nodeWorker struct {
	Nodes *MultiBlock
	XRange, YRange [2]float64
	Id int
}

func nodeWorkerLoop(workQueue chan chan *OSMPBF.PrimitiveBlock, quitChan chan bool, i int, xRange, yRange [2]float64, resultChan chan chan *MultiBlock) {
	w := &nodeWorker{
		Nodes: NewMultiBlock(),
		XRange: xRange,
		YRange: yRange,
		Id: i,
	}
	requestQueue := make(chan *OSMPBF.PrimitiveBlock)

	for {
		select {
		case workQueue <- requestQueue:
		case ch := <-resultChan:
			ch <- w.Nodes

		case <-quitChan:
			return
		}

		select {
		case work := <-requestQueue:
			w.processNodeRequest(work)

		case ch := <-resultChan:
			ch <- w.Nodes

		case <-quitChan:
			return
		}
	}
}

func (w *nodeWorker) processNodeRequest(b *OSMPBF.PrimitiveBlock) {
	for _, g := range b.Primitivegroup {
		for _, n := range g.Nodes {
			w.putNode(n.Id, int32(n.Lon), int32(n.Lat))
		}

		var id int64 = 0
		var lon int64 = 0
		var lat int64 = 0

		var last_i = 0
		for i, delta_id := range g.Dense.Id {
			if i < last_i {
				panic(fmt.Sprintf("Last index %d should be before current index %d.", last_i, i))
			}
			last_i = i

			id += delta_id
			lon += g.Dense.Lon[i]
			lat += g.Dense.Lat[i]

			w.putNode(id, int32(lon), int32(lat))
		}
	}
}

const (
	SCALE float64 = 1.0 / 10000000.0
	MAX_LAT int32 = int32(850511287)
)

func quadrant(coordRange [2]float64, coord float64) int {
	i := 4.0 * (coord - coordRange[0]) / (coordRange[1] - coordRange[0])
	if i >= 0.0 && i < 4.0 {
		return int(i)
	}
	return -1
}

func (w *nodeWorker) putNode(id int64, lon, lat int32) {
	if lat > -MAX_LAT && lat < MAX_LAT {
		p := geo.Point{float64(lon) * SCALE, float64(lat) * SCALE}
		geo.Mercator.Project(&p)

		x := quadrant(w.XRange, p.X())
		y := quadrant(w.YRange, p.Y())

		if x >= 0 && y >= 0 {
			mask := uint32(1) << uint32(x + 4 * y)
			w.Nodes.Append(id, mask)
		}
	}
}
