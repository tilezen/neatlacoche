package main

import "testing"

func TestAppend(t *testing.T) {
	tests := [][2]uint32{
		{0, 1},
		{1, 2},
		{2, (1<<16)-1},
		{(1<<16)-1, 7},
	}

	for _, kv := range tests {
		b := NewAccumulationBlock()
		if b.Length != 0 {
			t.Errorf("Expected length = 0, but length = %d", b.Length)
		}

		v := b.Lookup(kv[0])
		if v != 0 {
			t.Errorf("Lookup on empty array should be empty, not %d.", v)
		}

		b.Append(kv[0], kv[1])
		if b.Length != 1 {
			t.Errorf("Expected length = 1, but length = %d", b.Length)
		}

		v = b.Lookup(kv[0])
		if v != kv[1] {
			t.Errorf("After append, lookup should return %d, not %d.", kv[1], v)
		}
	}
}

func TestArrayMode(t *testing.T) {
	b := NewAccumulationBlock()

	for i := 0; i <= BLOCK_IDX_MASK; i += 1 {
		j := uint32(i)
		b.Append(j, j)
		if b.Length < j {
			t.Fatalf("Expected length >= %d, but was %d.", j, b.Length)
		}
		v := b.Lookup(j)
		if v != j {
			t.Fatalf("Unable to fetch value %d which we just appended, got %d instead.", j, v)
		}
	}

	for i := 0; i <= BLOCK_IDX_MASK; i += 1 {
		v := b.Lookup(uint32(i))
		if v != uint32(i) {
			t.Errorf("Unable to fetch value %d which was appended previously, got %d instead.", i, v)
		}
	}
}

func TestCopy(t *testing.T) {
	b := NewAccumulationBlock()

	for i := 0; i <= BLOCK_IDX_MASK; i += 1 {
		j := uint32(i)
		b.Append(j, j)
	}

	c := b.Copy()
	if c.Frozen != true {
		t.Fatalf("Expected copy of block to be frozen, but it isn't.")
	}

	for i := 0; i <= BLOCK_IDX_MASK; i += 1 {
		v := c.Lookup(uint32(i))
		if v != uint32(i) {
			t.Errorf("Unable to fetch value %d which was appended and copied, got %d instead.", i, v)
		}
	}
}

func TestCopyStride(t *testing.T) {
	b := NewAccumulationBlock()

	for i := 0; i < (1 << BLOCK_IDX_BITS); i += 10 {
		j := uint32(i)
		b.Append(j, uint32(i & BLOCK_VAL_MASK))
		b.Append(j, uint32((i + 1) & BLOCK_VAL_MASK))
	}

	c := b.Copy()
	if c.Frozen != true {
		t.Fatalf("Expected copy of block to be frozen, but it isn't.")
	}

	for i := 0; i < (1 << BLOCK_IDX_BITS); i += 10 {
		v := c.Lookup(uint32(i))
		expected := uint32(((i + 1) | i) & BLOCK_VAL_MASK)
		if v != expected {
			t.Errorf("Unable to fetch value %d which was appended and copied, got %d instead.", expected, v)
		}
	}
}

func TestNewEmptyBlock(t *testing.T) {
	b := NewEmptyBlock()
	if b.Frozen != true {
		t.Errorf("Expected empty block to be frozen, but wasn't.")
	}
	for i := 0; i < BLOCK_FULL_LENGTH; i += 1 {
		val := b.Lookup(uint32(i))
		if val != 0 {
			t.Fatalf("Expected lookup at %d to return zero, but got %d instead.", i, val)
		}
	}
}

func TestUnAppend(t *testing.T) {
	b := NewAccumulationBlock()
	b.Append(1, 1)
	b.Append(2, 2)
	idx, val := b.UnAppend()
	if idx != 2 || val != 2 {
		t.Fatalf("Expected to unappend (%d, %d), but got (%d, %d).", 2, 2, idx, val)
	}
	idx, val = b.UnAppend()
	if idx != 1 || val != 1 {
		t.Fatalf("Expected to unappend (%d, %d), but got (%d, %d).", 1, 1, idx, val)
	}
}

func TestCopyFrom(t *testing.T) {
	a := NewAccumulationBlock()
	b := NewAccumulationBlock()

	for i := 0; i < (1 << BLOCK_IDX_BITS); i += 10 {
		j := uint32(i)
		a.Append(j, uint32(i & BLOCK_VAL_MASK))
	}

	c := a.Copy()
	if c.Frozen != true {
		t.Fatalf("Expected c to be frozen, as it is a copy, but c.Frozen=%q.", c.Frozen)
	}

	b.CopyFrom(c)
	if c.Frozen != true {
		t.Fatalf("Expected b not to be frozen, as it is a copy-from, but b.Frozen=%q.", b.Frozen)
	}

	for i := 0; i < (1 << BLOCK_IDX_BITS); i += 10 {
		j := uint32(i)
		expected := uint32(i & BLOCK_VAL_MASK)
		val := b.Lookup(j)
		if val != expected {
			t.Fatalf("Expected lookup(%d) to return %d, but got %d instead.", j, expected, val)
		}
	}
}

func TestIterator(t *testing.T) {
	block := NewAccumulationBlock()

	vals := [...][2]uint32{
		{ 2, 15},
		{ 7,  1},
		{ 8, 10},
		{12,  5},
	}

	for _, a := range vals {
		block.Append(a[0], a[1])
	}

	itr := block.Iterator()
	for i, a := range vals {
		if !itr.Valid() {
			t.Fatalf("Expected (step %d) iterator to be valid.", i)
		}
		if itr.Index() != a[0] {
			t.Fatalf("Expected (step %d) iterator to have Index %d, but was %d.", i, a[0], itr.Index())
		}
		if itr.Value() != a[1] {
			t.Fatalf("Expected (step %d) iterator to have Value %d, but was %d.", i, a[1], itr.Value())
		}
		itr = itr.Next()
	}
	if itr.Valid() {
		t.Fatalf("Expected iterator to be invalid after %d steps.", len(vals))
	}
}

func TestIteratorDense(t *testing.T) {
	block := NewAccumulationBlock()

	for i := 1; i < (1 << BLOCK_IDX_BITS); i += 3 {
		block.Append(uint32(i), uint32(i) & BLOCK_VAL_MASK)
	}

	itr := block.Iterator()
	for i := 1; i < (1 << BLOCK_IDX_BITS); i += 3 {
		idx := uint32(i)
		val := uint32(i) & BLOCK_VAL_MASK

		if !itr.Valid() {
			t.Fatalf("Expected (step %d) iterator to be valid.", i)
		}
		if itr.Index() != idx {
			t.Fatalf("Expected (step %d) iterator to have Index %d, but was %d.", i, idx, itr.Index())
		}
		if itr.Value() != val {
			t.Fatalf("Expected (step %d) iterator to have Value %d, but was %d.", i, val, itr.Value())
		}
		itr = itr.Next()
	}
	if itr.Valid() {
		t.Fatalf("Expected iterator to be invalid after all steps.")
	}
}

func TestResetAndMergeFrom(t *testing.T) {
	a := NewAccumulationBlock()
	b := NewAccumulationBlock()

	for i := 0; i < (1 << BLOCK_IDX_BITS); i += 2 {
		j := uint32(i)
		a.Append(j, j & BLOCK_VAL_MASK)
		b.Append(j+1, (j+1) & BLOCK_VAL_MASK)
	}

	c := NewAccumulationBlock()
	c.ResetAndMergeFrom(a, b)

	for i := 0; i < (1 << BLOCK_IDX_BITS); i += 1 {
		j := uint32(i)
		expected := j & BLOCK_VAL_MASK
		val := c.Lookup(j)
		if val != expected {
			t.Fatalf("Expected lookup %d to return %d, but got %d.", j, expected, val)
		}
	}
}
