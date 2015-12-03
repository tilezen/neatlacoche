package main

import (
	"fmt"
)

// There are a few different designs which make sense for individual blocks. The
// one which is implemented here takes 16 bits for the lower bits of the ID and
// 16 for a 4x4 grid of bits for the grid squares.
//
// This can be represented in two ways, as a packed 32-bit int:
// `((id << 16) | val)` or as an array of 16-bit values of length (1<<16). It's
// possible to switch between them in such a way that the former is used until
// it reaches the size of the latter, after which the latter is used. This
// should ensure that it's efficient to represent both sparse and dense blocks.
//
// Inspiration for this comes from Daniel Lemire's "Roaring Bitmaps", simply
// extended to handle values: https://github.com/lemire/RoaringBitmap
//
// Other designs worth considering:
//
//   1. 28 bits for the ID, plus 4 bits (2x2) for the grid. This doesn't allow
//      as much fan-out for each process, but would be more efficient for sparse
//      blocks.
//
//   2. 28 bits for the ID, plus 36 bits (6x6) for the grid, packed into a
//      64-bit int. This allows more detail, but the grid size isn't a power of
//      two, which makes it less useful.
//
const (
	BLOCK_IDX_BITS = 16
	BLOCK_VAL_BITS = 16 // = 32 - BLOCK_IDX_BITS
	BLOCK_IDX_MASK = 0xFFFF // = (1 << BLOCK_IDX_BITS) - 1
	BLOCK_VAL_MASK = 0xFFFF // = (1 << BLOCK_VAL_BITS) - 1
	BLOCK_FULL_LENGTH = 1 << 15 // = (1 << BLOCK_IDX_BITS) / (32 / BLOCK_VAL_BITS)
	BLOCK_PACKING_BITS = 1 // = log2(32 / BLOCK_VAL_BITS)
	BLOCK_PACKING_MASK = 1 // = (1 << BLOCK_VAL_BITS) - 1
)

// The Block structure handles a single block, either packed as "list-of-pairs"
// or an array of 16-bit ints (packed into 32-bit ints).
type Block struct {
	// Length tracks either the number of pairs present in the list-of-pairs mode
	// or, if > BLOCK_FULL_LENGTH, indicates that the Block is in array mode.
	Length uint32

	// Frozen is true if the Block is immutable.
	Frozen bool

	// Values contains the packed list-of-pairs or array of grid bitfields.
	Values []uint32
}

// NewAccumulationBlock returns a full pre-allocated block. This means it will
// not need to grow as new values are added to it. It is intended for use as an
// accumulation buffer, so that blocks can be copied from it and it can be reset
// to accumulate the next. This avoids the need for reallocations and reduces GC
// pressure.
func NewAccumulationBlock() *Block {
	return &Block{
		Length: 0,
		Frozen: false,
		Values: make([]uint32, BLOCK_FULL_LENGTH)}
}

// NewEmptyBlock returns a new, empty, frozen block. This should be okay to do
// lookups on, but is otherwise a "null" value.
func NewEmptyBlock() *Block {
	return &Block{
		Length: 0,
		Frozen: true,
		Values: make([]uint32, 0)}
}

// Copy copies a block, allocating only the memory needed to represent what's in
// the block. The new block is frozen, and cannot be mutated.
func (b *Block) Copy() *Block {
	nb := new(Block)
	nb.Length = b.Length
	nb.Frozen = true

	if b.Length > BLOCK_FULL_LENGTH {
		nb.Values = make([]uint32, BLOCK_FULL_LENGTH)

	} else {
		nb.Values = make([]uint32, b.Length)
	}

	copy(nb.Values, b.Values)

	return nb
}

func writePacked(arr []uint32, id, val uint32) {
	hilo := id & BLOCK_PACKING_MASK
	idx := id >> BLOCK_PACKING_BITS
	arr[idx] = arr[idx] | (val << (hilo * BLOCK_VAL_BITS))
}

// Append an (id, val) pair onto the end of the block. The id must be unique,
// and greater than any other id appended to this block before. The val must fit
// into the values allowed in this block (i.e: BLOCK_VAL_BITS bits).
func (b *Block) Append(id uint32, val uint32) {
	// sanity checking
	if id > BLOCK_IDX_MASK {
		panic(fmt.Sprintf("ID value %d is too large for this block, max is %d.", id, BLOCK_IDX_MASK))
	}
	if val > BLOCK_VAL_MASK {
		panic(fmt.Sprintf("Val value %d is too large for this block, max is %d.", val, BLOCK_VAL_MASK))
	}
	if b.Frozen {
		panic("Attempt to append to a frozen Block, which is not allowed.")
	}

	if b.Length > BLOCK_FULL_LENGTH {
		// block is in array mode
		if id >= b.Length {
			panic(fmt.Sprintf("Unable to push %d into array-mode block of size %d.", id, b.Length))
		}

		writePacked(b.Values, id, val)

	} else if b.Length < BLOCK_FULL_LENGTH {
		// block is in list-of-pair mode
		b.Values[b.Length] = (id << BLOCK_VAL_BITS) | val
		b.Length += 1

	} else {
		// block _was_ in list-of-pair mode, but now needs
		// to transition to array mode.
		var tmp [BLOCK_FULL_LENGTH]uint32
		for _, kv := range b.Values {
			k := kv >> BLOCK_VAL_BITS
			v := kv & BLOCK_VAL_MASK

			writePacked(tmp[:], k, v)
		}

		writePacked(tmp[:], id, val)

		copy(b.Values, tmp[:])
		b.Length = 1 << BLOCK_IDX_BITS
	}
}

// Reset the block to an empty state. Note that it doesn't reallocate any
// memory.
func (b *Block) Reset() {
	if b.Frozen {
		panic("Attempt to reset a frozen Block, which is not allowed.")
	}

	b.Length = 0
	// zero out the slice. this shouldn't really be necessary, but is probably
	// worth keeping until at least more sure that the rest of the code is
	// working.
	for i := range b.Values {
		b.Values[i] = 0
	}
}

// Simple binary search on the upper bits of the Values array, used when the
// Block is in list-of-pairs mode.
func search(arr []uint32, lb uint32) uint32 {
	if len(arr) == 0 {
		return 0
	} else if len(arr) == 1 {
		return arr[0]
	}

	mididx := len(arr) / 2
	mid := arr[mididx] >> BLOCK_VAL_BITS

	if lb < mid {
		return search(arr[:mididx], lb)
	} else {
		return search(arr[mididx:], lb)
	}
}

// Lookup an ID, returning the value (grid bitfield) associated with it, or
// zero if the ID wasn't found.
func (b *Block) Lookup(id uint32) uint32 {
	// sanity checking
	if id > BLOCK_IDX_MASK {
		panic(fmt.Sprintf("Lookup value %d is larger than max %d.", id, BLOCK_IDX_MASK))
	}

	if b.Length > BLOCK_FULL_LENGTH {
		// in array mode
		hilo := id & BLOCK_PACKING_MASK
		idx := id >> BLOCK_PACKING_BITS
		return (b.Values[idx] >> (hilo * BLOCK_VAL_BITS)) & BLOCK_VAL_MASK

	} else {
		// in list-of-pairs mode
		lb := search(b.Values[:b.Length], id)

		if (lb >> BLOCK_VAL_BITS) == id {
			return lb & BLOCK_VAL_MASK

		} else {
			return 0
		}
	}
}

// Take the last appended value off the Block, returning it. Note that this
// won't trigger a "shrink" of the Block back to list-of-pair mode if it's in
// array mode, and is intended to be used rarely - it's not implemented
// particularly efficiently.
func (b *Block) UnAppend() (idx, val uint32) {
	if b.Frozen {
		panic("Attempt to unappend from a frozen Block, which is not allowed.")
	}

	if b.Length > BLOCK_FULL_LENGTH {
		// block is in array mode
		// TODO: find a better algorithm than brute force backward search for this?
		// UnAppend is pretty rare...
	Loop:
		for i := BLOCK_FULL_LENGTH - 1; i >= 0; i -= 1 {
			v := b.Values[i]
			if v > 0 {
				for j := BLOCK_PACKING_MASK; j >= 0; j -= 1 {
					vj := (v >> (uint(j) * BLOCK_VAL_BITS)) & BLOCK_VAL_MASK
					if vj > 0 {
						idx = uint32((i << BLOCK_PACKING_BITS) | j)
						val = vj
						break Loop
					}
				}
			}
		}
		// NOTE: won't trigger a "shrink" from array mode back to list-of-pair mode.

	} else if b.Length > 0 {
		// block is in list-of-pair mode
		b.Length -= 1
		kv := b.Values[b.Length]
		idx = kv >> BLOCK_VAL_BITS
		val = kv & BLOCK_VAL_MASK
	}

	if idx > BLOCK_IDX_MASK {
		panic(fmt.Sprintf("Block index %d out of range, max is %d.", idx, BLOCK_IDX_MASK))
	}
	if val > BLOCK_VAL_MASK {
		panic(fmt.Sprintf("Block value %d out of range, max is %d.", val, BLOCK_VAL_MASK))
	}

	return
}

// CopyFrom another block. This can be used to "unfreeze" a frozen Block by
// copying it into an accumulation Block.
func (b *Block) CopyFrom(b2 *Block) {
	if b.Frozen {
		panic("Attempt to copy into a frozen Block, which is not allowed.")
	}

	b.Reset()

	if len(b.Values) < len(b2.Values) {
		panic(fmt.Sprintf("Unable to copy %d bytes into smaller Block of %d bytes.", len(b2.Values), len(b.Values)))
	}

	copy(b.Values, b2.Values)
	b.Length = b2.Length
}

// Iterator allows read-only access to the values in a Block by a uniform
// interface, which is used in the Block merging functions.
type Iterator struct {
	block *Block
	idx int
}

// Valid returns true when the Iterator is valid; when Index and Value can
// be called.
func (i Iterator) Valid() bool {
	return uint32(i.idx) < i.block.Length
}

// Index returns the ID of the record that the Iterator is currently pointing
// to. The Iterator *must* be Valid, or this might cause a panic.
func (i Iterator) Index() uint32 {
	if i.block.Length > BLOCK_FULL_LENGTH {
		// array mode
		return uint32(i.idx)

	} else {
		// list-of-pairs mode
		return i.block.Values[i.idx] >> BLOCK_VAL_BITS
	}
}

// Value returns the value of the record that the Iterator is currently pointing
// to. The Iterator *must* be Valid, or this might cause a panic.
func (i Iterator) Value() uint32 {
	if i.block.Length > BLOCK_FULL_LENGTH {
		// array mode
		hilo := uint32(i.idx) & BLOCK_PACKING_MASK
		idx := i.idx >> BLOCK_PACKING_BITS
		return (i.block.Values[idx] >> (hilo * BLOCK_VAL_BITS)) & BLOCK_VAL_MASK

	} else {
		// list-of-pairs mode
		return i.block.Values[i.idx] & BLOCK_VAL_MASK
	}
}

// Next increments the Iterator to point to the next record. You should check
// whether the Iterator is still Valid after calling this.
func (i Iterator) Next() Iterator {
	if i.block.Length > BLOCK_FULL_LENGTH {
		// array mode
		idx := i.idx >> BLOCK_PACKING_BITS
		for j := idx; j < BLOCK_FULL_LENGTH; j += 1 {
			v := i.block.Values[j]
			if v > 0 {
				for k := 0; k <= BLOCK_PACKING_MASK; k += 1 {
					vj := (v >> (uint(k) * BLOCK_VAL_BITS)) & BLOCK_VAL_MASK
					if vj > 0 {
						ii := (j << BLOCK_PACKING_BITS) | k
						if ii > i.idx {
							return Iterator{block: i.block, idx: ii}
						}
					}
				}
			}
		}

		return Iterator{block: i.block, idx: (1 << BLOCK_IDX_BITS)}

	} else {
		// list-of-pairs mode
		return Iterator{block: i.block, idx: i.idx + 1}
	}
}

// Iterator returns an Iterator pointing to the beginning of the Block.
func (b *Block) Iterator() Iterator {
	return Iterator{block: b, idx: 0}
}

// ResetAndMergeFrom resets the receiver accumulation block and fills it with
// data from block1 and block2. In other words; if (id, val) was a record in
// either block1 or block2, then (id, val | c) will be a record in the receiver
// for some constant c (it might be OR-ed with something from the other Block).
// This is done in a single pass, so should be relatively efficient.
func (b *Block) ResetAndMergeFrom(block1, block2 *Block) {
	b.Reset()

	it1 := block1.Iterator()
	it2 := block2.Iterator()

	for it1.Valid() && it2.Valid() {
		if it1.Index() < it2.Index() {
			b.Append(it1.Index(), it1.Value())
			it1 = it1.Next()

		} else if it1.Index() == it2.Index() {
			b.Append(it1.Index(), it1.Value() | it2.Value())
			it1 = it1.Next()
			it2 =	it2.Next()

		} else {
			b.Append(it2.Index(), it2.Value())
			it2 = it2.Next()
		}
	}

	// Note that only one of it1, it2 can be valid at this point because the loop
	// terminated. But there might still be some values in one or the other, so
	// they need to be drained too.
	for it1.Valid() {
		b.Append(it1.Index(), it1.Value())
		it1 = it1.Next()
	}

	for it2.Valid() {
		b.Append(it2.Index(), it2.Value())
		it2 = it2.Next()
	}
}
