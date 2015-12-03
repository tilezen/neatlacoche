package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
)

// FirstPass over the input file to figure out which grid square each node, way,
// relation, etc... needs to go into. This info is kept in memory, but no other
// details about the item, and used in the second pass to actually create the
// file. This ensures that the output files are ordered, same as the input file,
// and means we're not building a huge database.
func FirstPass(file_name string) (*Sorter, error) {
	reader, err := NewPBFReader(file_name)
	if err != nil {
		return nil, fmt.Errorf("Unable to open %q: %s\n", file_name, err.Error())
	}
	defer reader.Close()

	// ReadHeaderBlock does some internal checks so, at this stage,
	// we don't actually need the information in it.
	_, err = reader.ReadHeaderBlock()
	if err != nil {
		return nil, fmt.Errorf("Unable to read header block: %s", err.Error())
	}

	// The Sorter object sorts each item into one of several grid squares - at the
	// moment hard-coded to the world extent.
	merc_extent := [2]float64{-20037508.34, 20037508.34}
	sorter, err := NewSorter(runtime.NumCPU(), merc_extent, merc_extent)
	if err != nil {
		return nil, fmt.Errorf("Unable to construct a Sorter object: %s", err.Error())
	}

	// Read through the file, keeping any error for the end. It's running a bunch
	// of goroutines in the reader and the Sorter, and shutting that down properly
	// and cleanly is a TODO.
	for block_or_error := range reader.ReadBlocks() {
		if block_or_error.Err != nil {
			err = block_or_error.Err
		} else if err == nil {
			sorter.Append(block_or_error.Primitives)
		}
	}

	if err != nil {
		return nil, err
	}

	return sorter, nil
}

var cpuprofile = flag.String("cpuprofile", "", "Write CPU profile to this file")

// Used to stuff all this into a LevelDB, but that was pretty slow. Might want
// to try that again later for handling updates, though.
//var db_file_name = flag.String("db-file", "my.db", "LevelDB database to use")

func main() {
	flag.Parse()

	file_name := flag.Arg(0)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatalf("Unable to write to CPU profile %q: %s\n", *cpuprofile, err.Error())
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	sorter, err := FirstPass(file_name)
	if err != nil {
		log.Fatalf("Failed during the first pass: %s\n", err.Error())
	}
	defer sorter.Close()

	fmt.Printf("All done.\n")
}
