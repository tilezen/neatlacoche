package main

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"github.com/mapzen/neatlacoche/OSMPBF"
	"io"
	"os"
	"runtime"
)

//go:generate protoc --gogo_out=$GOPATH/src/github.com/mapzen/neatlacoche -I$GOPATH/src/github.com/mapzen/neatlacoche:$GOPATH/src:$GOPATH/src/github.com/gogo/protobuf/protobuf $GOPATH/src/github.com/mapzen/neatlacoche/OSMPBF/fileformat.proto
//go:generate protoc --gogo_out=$GOPATH/src/github.com/mapzen/neatlacoche -I$GOPATH/src/github.com/mapzen/neatlacoche:$GOPATH/src:$GOPATH/src/github.com/gogo/protobuf/protobuf $GOPATH/src/github.com/mapzen/neatlacoche/OSMPBF/osmformat.proto

type Unmarshaller interface {
	Unmarshal(data []byte) error
}

type PBFReader struct {
	file *os.File
}

func NewPBFReader(file_name string) (reader *PBFReader, err error) {
	file, err := os.Open(file_name)
	if err != nil {
		return
	}
	reader = new(PBFReader)
	reader.file = file
	return
}

func (r *PBFReader) Close() {
	r.file.Close()
}

func readBlobHeader(file *os.File) (header OSMPBF.BlobHeader, data_offset int64, err error) {
	var length uint32 = 0

	err = binary.Read(file, binary.BigEndian, &length)
	if err == io.EOF {
		return

	} else if err != nil {
		err = fmt.Errorf("ReadBlobHeader: Could not read next blob header length: %s\n", err.Error())
		return
	}

	buf := make([]byte, length, length)
	_, err = io.ReadFull(file, buf)
	if err != nil {
		err = fmt.Errorf("ReadBlobHeader: Could not read blob header: %s\n", err.Error())
		return
	}

	err = header.Unmarshal(buf)
	if err != nil {
		err = fmt.Errorf("ReadBlobHeader: Could not unmarshal blob header: %s\n", err.Error())
		return
	}

	data_offset, err = file.Seek(0, 1) // get current offset
	if err != nil {
		err = fmt.Errorf("ReadBlobHeader: Could not get current offset: %s\n", err.Error())
		return
	}

	_, err = file.Seek(int64(header.Datasize), 1)
	if err != nil {
		err = fmt.Errorf("ReadBlobHeader: Could not skip to next header: %s\n", err.Error())
		return
	}

	return
}

func readBlob(file *os.File, data_size int32, offset int64, obj Unmarshaller) error {
	buf := make([]byte, data_size, data_size)
	_, err := file.ReadAt(buf, offset)
	if err != nil {
		return fmt.Errorf("ReadBlob: Unable to read first blob: %s\n", err.Error())
	}

	var blob OSMPBF.Blob
	err = blob.Unmarshal(buf)
	if err != nil {
		return fmt.Errorf("ReadBlob: Unable to unmarshal Blob: %s\n", err.Error())
	}

	if len(blob.Raw) > 0 {
		err = obj.Unmarshal(blob.Raw)

	} else if len(blob.ZlibData) > 0 {
		raw_reader := bytes.NewReader(blob.ZlibData)
		zlib_reader, err := zlib.NewReader(raw_reader)
		if err == nil {
			buf := make([]byte, blob.RawSize, blob.RawSize)
			_, err = io.ReadFull(zlib_reader, buf)
			if err == nil {
				err = obj.Unmarshal(buf)
			}
		}

	} else {
		return fmt.Errorf("ReadBlob: Unsupported compression type in block, this program only currently supports uncompressed and gzip compressed blobs.")
	}
	if err != nil {
		return fmt.Errorf("ReadBlob: Unable to decode header block: %s\n", err.Error())
	}

	return nil
}

func (r *PBFReader) ReadHeaderBlock() (header_block *OSMPBF.HeaderBlock, err error) {
	header, offset, err := readBlobHeader(r.file)
	if err != nil {
		err = fmt.Errorf("ReadHeaderBlock: Unable to read PBF file header: %s\n", err.Error())
		return
	}
	if header.Type != "OSMHeader" {
		err = fmt.Errorf("ReadHeaderBlock: Expected first blob in PBF file to be a header, but it was a %q.\n", header.Type)
		return
	}

	header_block = new(OSMPBF.HeaderBlock)
	err = readBlob(r.file, header.Datasize, offset, header_block)
	if err != nil {
		err = fmt.Errorf("ReadHeaderBlock: could not read Blob: %s", err.Error())
	}

	for _, required_feature := range header_block.RequiredFeatures {
		if required_feature != "OsmSchema-V0.6" &&
			required_feature != "DenseNodes" &&
			required_feature != "HistoricalInformation" {
			err = fmt.Errorf("ReadHeaderBlock: Required feature %q is not supported by this program.\n", required_feature)
			return
		}
	}

	return
}

type BlockOrError struct {
	Primitives *OSMPBF.PrimitiveBlock
	Err        error
}

func (r *PBFReader) ReadBlocks() <-chan BlockOrError {
	queue := make(chan chan BlockOrError, runtime.NumCPU())
	out := make(chan BlockOrError, runtime.NumCPU())

	go readBlockConsumer(queue, out)
	go readBlockProducer(r.file, queue)

	return out
}

func readBlockConsumer(in <-chan chan BlockOrError, out chan<- BlockOrError) {
	defer close(out)

	for ch := range in {
		for block_or_error := range ch {
			out <- block_or_error
		}
	}
}

func chanError(err error) chan BlockOrError {
	ch := make(chan BlockOrError)
	go func(e error, c chan BlockOrError) { ch <- BlockOrError{Err: e} }(err, ch)
	return ch
}

func readBlockProducer(file *os.File, out chan<- chan BlockOrError) {
	defer close(out)

	for {
		header, offset, err := readBlobHeader(file)
		if err == io.EOF {
			break

		} else if err != nil {
			out <- chanError(fmt.Errorf("ReadBlocks: Unable to read PBF file header: %s\n", err.Error()))
			return
		}

		if header.Type != "OSMData" {
			out <- chanError(fmt.Errorf("ReadBlocks: Expected data blob in PBF file, but it was a %q.\n", header.Type))
			return
		}

		ch := make(chan BlockOrError)
		go readDataBlock(file, header.Datasize, offset, ch)
		out <- ch
	}
}

func primCount(p *OSMPBF.PrimitiveBlock) (nodes, ways, rels int) {
	for _, g := range p.Primitivegroup {
		nodes += len(g.Nodes) + len(g.Dense.Id)
		ways += len(g.Ways)
		rels += len(g.Relations)
	}
	return
}

func primBlockSplit(p *OSMPBF.PrimitiveBlock) (nodes, ways, rels *OSMPBF.PrimitiveBlock) {
	n, w, r := primCount(p)

	if n > 0 {
		nodes = new(OSMPBF.PrimitiveBlock)
		nodes.Strings = p.Strings
		nodes.Granularity = p.Granularity
		nodes.LatOffset = p.LatOffset
		nodes.LonOffset = p.LonOffset
		nodes.DateGranularity = p.DateGranularity

		for _, g := range p.Primitivegroup {
			if len(g.Nodes) > 0 {
				nodes.Primitivegroup = append(nodes.Primitivegroup, OSMPBF.PrimitiveGroup{Nodes: g.Nodes})
			}
			if len(g.Dense.Id) > 0 {
				nodes.Primitivegroup = append(nodes.Primitivegroup, OSMPBF.PrimitiveGroup{Dense: g.Dense})
			}
		}
	}

	if w > 0 {
		ways = new(OSMPBF.PrimitiveBlock)
		ways.Strings = p.Strings
		ways.Granularity = p.Granularity
		ways.LatOffset = p.LatOffset
		ways.LonOffset = p.LonOffset
		ways.DateGranularity = p.DateGranularity

		for _, g := range p.Primitivegroup {
			if len(g.Ways) > 0 {
				ways.Primitivegroup = append(ways.Primitivegroup, OSMPBF.PrimitiveGroup{Ways: g.Ways})
			}
		}
	}

	if r > 0 {
		rels = new(OSMPBF.PrimitiveBlock)
		rels.Strings = p.Strings
		rels.Granularity = p.Granularity
		rels.LatOffset = p.LatOffset
		rels.LonOffset = p.LonOffset
		rels.DateGranularity = p.DateGranularity

		for _, g := range p.Primitivegroup {
			if len(g.Relations) > 0 {
				rels.Primitivegroup = append(rels.Primitivegroup, OSMPBF.PrimitiveGroup{Relations: g.Relations})
			}
		}
	}

	return
}

func readDataBlock(file *os.File, data_size int32, offset int64, ch chan<- BlockOrError) {
	block := new(OSMPBF.PrimitiveBlock)
	defer close(ch)

	err := readBlob(file, data_size, offset, block)
	if err != nil {
		ch <- BlockOrError{Err: err}

	} else {
		nodes, ways, rels := primCount(block)

		numTypes := 0
		if nodes > 0 {
			numTypes += 1
		}
		if ways > 0 {
			numTypes += 1
		}
		if rels > 0 {
			numTypes += 1
		}

		if numTypes <= 1 {
			ch <- BlockOrError{Primitives: block}

		} else {
			nodeBlock, wayBlock, relBlock := primBlockSplit(block)
			if nodeBlock != nil {
				ch <- BlockOrError{Primitives: nodeBlock}
			}
			if wayBlock != nil {
				ch <- BlockOrError{Primitives: wayBlock}
			}
			if relBlock != nil {
				ch <- BlockOrError{Primitives: relBlock}
			}
		}
	}
}
