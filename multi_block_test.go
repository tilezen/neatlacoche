package main

import "testing"

func TestMultiBlock(t *testing.T) {
	mb := NewMultiBlock()

	for i := 0; i < 100 * BLOCK_FULL_LENGTH; i += 10 {
		mb.Append(int64(i), uint32(i & BLOCK_VAL_MASK))
		mb.Append(int64(i), uint32((i + 1) & BLOCK_VAL_MASK))

		val := mb.Lookup(int64(i))
		expected := uint32(((i + 1) | i) & BLOCK_VAL_MASK)
		if val != expected {
			t.Fatalf("After just writing, expected %d at index %d, but got %d instead.", expected, i, val)
		}
	}

	for i := 0; i < 100 * BLOCK_FULL_LENGTH; i += 10 {
		val := mb.Lookup(int64(i))
		expected := uint32(((i + 1) | i) & BLOCK_VAL_MASK)
		if val != expected {
			t.Fatalf("In second loop, expected %d at index %d, but got %d instead.", expected, i, val)
		}
	}
}

/*
func TestMultiBlockMergeAlternate(t *testing.T) {
	mb := NewMultiBlock()
	l := NewMultiBlock()
	r := NewMultiBlock()

	for i := 0; i < 10 * BLOCK_FULL_LENGTH; i += 2 {
		l.Append(int64(i),   uint32(5))
		r.Append(int64(i+1), uint32(10))
	}

	mb.Merge(l)
	mb.Merge(r)
	for i := 0; i < 10 * BLOCK_FULL_LENGTH; i += 2 {
		val_l := mb.Lookup(int64(i))
		val_r := mb.Lookup(int64(i+1))
		if val_l != uint32(5) {
			t.Fatalf("Expected value at %d to be 5, but was %d.", i, val_l)
		}
		if val_r != uint32(10) {
			t.Fatalf("Expected value at %d to be 10, but was %d.", i, val_r)
		}
	}
}

func TestMultiBlockMerge(t *testing.T) {
	mb := NewMultiBlock()
	l := NewMultiBlock()
	r := NewMultiBlock()

	for i := 0; i < 10 * BLOCK_FULL_LENGTH; i += 1 {
		l.Append(int64(i),   uint32(5))
		r.Append(int64(i), uint32(10))
	}

	mb.Merge(l)
	mb.Merge(r)
	for i := 0; i < 10 * BLOCK_FULL_LENGTH; i += 1 {
		val := mb.Lookup(int64(i))
		if val != uint32(15) {
			t.Fatalf("Expected value at %d to be 15, but was %d.", i, val)
		}
	}
}
*/
