package main

import (
	"fmt"
	"sort"
)

// MultiBlock is a mapping from an element ID to a bitmask of which grid squares
// it's in. This data structure attempts to be memory and write-friendly by
// taking advantage of a few special features of the data:
//
//   1. When iterating over elements in the file, they always come in ascending
//      (ID, version) order. Therefore we only need to append new items, not
//      insert items in the middle of the data structure.
//
//   2. Follows from (1); IDs are mostly contiguous and clustered towards the
//      low end of the 64-bit numeric space, therefore the top bits of the ID
//      are very likely to be zero.
//
//   3. We don't care about the version, only the ID. This means we can collapse
//      several contiguous records together.
//
//   4. We are outputting to a small number (BLOCK_VAL_BITS) of output grid
//      squares, so we can compress that down - see Block for more info about
//      that.
//
type MultiBlock struct {
	// Map the top (64 - BLOCK_IDX_BITS) bits of the ID to the block containing
	// them. Because of reason (2), we expect that there will be relatively few
	// of these, and therefore the map structure will be small relative to the
	// Blocks pointed to. Blocks in this map are "frozen" and cannot be changed.
	Blocks map[int64]*Block

	// Because of reasons (1) and (3), we accumulate data into a pre-allocated
	// "current" block, which is the only mutable block. This one grows with the
	// appended data until an ID is seen which is not in this block. At that
	// point we know that the block is complete and it is added to the Blocks
	// map.
	Current *Block

	// Last ID and value (OR-ed collection of grid squares) seen. This is used
	// mainly to collapse down versions of the same ID efficiently.
	LastId int64
	LastVal uint32
}

func NewMultiBlock() *MultiBlock {
	return &MultiBlock{
		Blocks: make(map[int64]*Block),
		Current: NewAccumulationBlock(),
		LastId: 0,
		LastVal: 0,
	}
}

// Append an (ID, grid square) to the data structure.
func (m *MultiBlock) Append(id int64, val uint32) {
	if id < m.LastId {
		panic(fmt.Sprintf("ID %d < last ID %d, but IDs must be in order!", id, m.LastId))
	}
	if val > BLOCK_VAL_MASK {
		panic(fmt.Sprintf("Can't append a value of %d, max is %d.", val, BLOCK_VAL_MASK))
	}

	// Reason (3) - just collapse all the items with the same ID down into a
	// single record.
	if id == m.LastId {
		m.LastVal = m.LastVal | val

	} else {
		// The ID is different (must be greater - see previous checks on id), so we
		// first need to flush the data in the Last* variables to the Current block.
		m.Current.Append(uint32(m.LastId & BLOCK_IDX_MASK), m.LastVal)

		// Then we check if the Current block needs to be pushed back onto the
		// Blocks map.
		upper := int64(id >> BLOCK_IDX_BITS)
		lastUpper := int64(m.LastId >> BLOCK_IDX_BITS)
		if upper != lastUpper {
			block := m.Current.Copy()
			m.Current.Reset()
			m.Blocks[lastUpper] = block
		}

		// Finally, assign the Last* variables to the new values.
		m.LastId = id
		m.LastVal = val
	}
}

// Use the maximum int64 value as a marker that the multi-block is currently
// 'pushed', and can't be appended to. The 'pushed' status should be
// temporary, but using the max value ensures that other operations won't
// corrupt the data structure if somehow it's left in a 'pushed' state.
const maxLastId = int64((^uint64(0)) >> 1)

// pushCurrent puts the "tail" of the data structure, the Current block and
// LastId/LastVal, onto the main blocks structure. This makes the data structure
// more uniform and easier to perform some operations on.
func (m *MultiBlock) pushCurrent() {
	// push LastId/LastVal into the end of the current block
	m.Current.Append(uint32(m.LastId & BLOCK_IDX_MASK), m.LastVal)

	// push the Current block onto the Blocks map
	lastUpper := int64(m.LastId >> BLOCK_IDX_BITS)
	block := m.Current.Copy()
	m.Current.Reset()
	m.Blocks[lastUpper] = block

	// reset LastId/LastVal to nonsense values. this will prevent any other
	// operation (e.g: Append) from working while the data structure is "pushed".
	m.LastId = maxLastId
	m.LastVal = 0
}

// Implement the sort.Interface interface for slices of int64s so that we can
// sort them generically.
type int64slice []int64
func (a int64slice) Len() int {
	return len(a)
}
func (a int64slice) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a int64slice) Less(i, j int) bool {
	return a[i] < a[j]
}

// sortedBlockKeys returns an array of all the block keys sorted in ascending
// numerical order.
func (m *MultiBlock) sortedBlockKeys() []int64 {
	uppers := make([]int64, len(m.Blocks))
	i := 0
	for k, _ := range m.Blocks {
		uppers[i] = k
		i += 1
	}
	sort.Sort(int64slice(uppers))
	return uppers
}

// unPushCurrent reverses the changes made by pushCurrent. That is, it takes the
// last block and unpacks it into the Current block and LastId/LastVal.
func (m *MultiBlock) unPushCurrent() {
	blockKeys := m.sortedBlockKeys()

	if len(blockKeys) > 0 {
		lastKey := blockKeys[len(blockKeys)-1]
		lastBlock := m.Blocks[lastKey]
		delete(m.Blocks, lastKey)
		m.Current.CopyFrom(lastBlock)
		lastIdx, lastVal := m.Current.UnAppend()
		m.LastId = (lastKey << BLOCK_IDX_BITS) | int64(lastIdx)
		m.LastVal = lastVal

	} else {
		m.Current.Reset()
		m.LastId, m.LastVal = 0, 0
	}
}

// Lookup a value in the data structure, returning the grid square bitfield
// value, or zero if the ID cannot be found.
func (m *MultiBlock) Lookup(id int64) uint32 {
	if id == m.LastId {
		return m.LastVal
	}

	upper := id >> BLOCK_IDX_BITS
	lastUpper := int64(m.LastId >> BLOCK_IDX_BITS)
	blockIdx := uint32(id & BLOCK_IDX_MASK)

	if upper == lastUpper {
		return m.Current.Lookup(blockIdx)

	} else if block, ok := m.Blocks[upper]; ok {
		return block.Lookup(blockIdx)
	}

	return 0
}

// Merge the mb2 data structure into the receiver (mb). This can be done
// efficiently, as both are in sorted order. Note that this operation will
// destroy mb2.
func (mb *MultiBlock) Merge(mb2 *MultiBlock) {
	new_block := NewAccumulationBlock()
	mb.pushCurrent()
	mb2.pushCurrent()

	for upper, block2 := range mb2.Blocks {
		if block, ok := mb.Blocks[upper]; ok {
			// existing block, merge the two blocks
			new_block.ResetAndMergeFrom(block, block2)
			mb.Blocks[upper] = new_block.Copy()

		} else {
			// no existing block, can just take the other
			mb.Blocks[upper] = block2
		}
	}

	mb.unPushCurrent()

	// blank the merged multi-block, since we might have taken some of its
	// internal structures.
	mb2.Blocks = map[int64]*Block{}
	mb2.Current = NewEmptyBlock()
	mb2.LastId = 0
	mb2.LastVal = 0
}
