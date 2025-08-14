package engine

import (
	"testing"
)

func TestBitsPerLevel(t *testing.T) {
	tests := []struct {
		fanout uint32
		want   uint
	}{
		{2, 1},  // 2^1 = 2
		{4, 2},  // 2^2 = 4
		{8, 3},  // 2^3 = 8
		{16, 4}, // 2^4 = 16
		{32, 5}, // 2^5 = 32
		{64, 6}, // 2^6 = 64
	}

	for _, tt := range tests {
		got := bitsPerLevel(tt.fanout)
		if got != tt.want {
			t.Errorf("bitsPerLevel(%d) = %d, want %d", tt.fanout, got, tt.want)
		}
	}
}

func TestBitsPerLevelPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("bitsPerLevel should panic for non-power-of-two fanout")
		}
	}()

	bitsPerLevel(3) // This should panic
}

func TestBitPosition(t *testing.T) {
	tests := []struct {
		pos          uint
		bpl          uint
		wantStartBit uint
		wantByteIdx  uint
		wantOff      uint
	}{
		{0, 4, 0, 0, 0},  // first digit at bit 0
		{1, 4, 4, 0, 4},  // second digit at bit 4
		{2, 4, 8, 1, 0},  // third digit at bit 8 (next byte)
		{3, 4, 12, 1, 4}, // fourth digit at bit 12
	}

	for _, tt := range tests {
		startBit, byteIdx, off := bitPosition(tt.pos, tt.bpl)
		if startBit != tt.wantStartBit || byteIdx != tt.wantByteIdx || off != tt.wantOff {
			t.Errorf("bitPosition(%d, %d) = (%d, %d, %d), want (%d, %d, %d)",
				tt.pos, tt.bpl, startBit, byteIdx, off,
				tt.wantStartBit, tt.wantByteIdx, tt.wantOff)
		}
	}
}

func TestCreateBitWindow(t *testing.T) {
	buf := []byte{0x12, 0x34, 0x56, 0x78}

	// Test window at byte 0
	w := createBitWindow(buf, 0)
	expected := uint16(0x12)<<8 | uint16(0x34)
	if w != expected {
		t.Errorf("createBitWindow(buf, 0) = 0x%04x, want 0x%04x", w, expected)
	}

	// Test window at byte 1
	w = createBitWindow(buf, 1)
	expected = uint16(0x34)<<8 | uint16(0x56)
	if w != expected {
		t.Errorf("createBitWindow(buf, 1) = 0x%04x, want 0x%04x", w, expected)
	}

	// Test window at end of buffer
	w = createBitWindow(buf, 3)
	expected = uint16(0x78) << 8
	if w != expected {
		t.Errorf("createBitWindow(buf, 3) = 0x%04x, want 0x%04x", w, expected)
	}
}

func TestCalculateMaskAndShift(t *testing.T) {
	tests := []struct {
		off       uint
		bpl       uint
		wantMask  uint16
		wantShift uint
	}{
		{0, 4, 0x0F, 12}, // 4 bits at MSB
		{4, 4, 0x0F, 8},  // 4 bits at middle
		{8, 4, 0x0F, 4},  // 4 bits at LSB
		{0, 2, 0x03, 14}, // 2 bits at MSB
	}

	for _, tt := range tests {
		mask, shift := calculateMaskAndShift(tt.off, tt.bpl)
		if mask != tt.wantMask || shift != tt.wantShift {
			t.Errorf("calculateMaskAndShift(%d, %d) = (0x%02x, %d), want (0x%02x, %d)",
				tt.off, tt.bpl, mask, shift, tt.wantMask, tt.wantShift)
		}
	}
}

func TestEnsureBufferSize(t *testing.T) {
	// Test buffer expansion
	buf := []byte{0x12, 0x34}
	needBits := uint(24) // need 3 bytes
	expanded := ensureBufferSize(buf, needBits)

	if len(expanded) != 3 {
		t.Errorf("ensureBufferSize expanded buffer to %d bytes, want 3", len(expanded))
	}

	// Check original data is preserved
	if expanded[0] != 0x12 || expanded[1] != 0x34 {
		t.Error("ensureBufferSize did not preserve original data")
	}

	// Test no expansion needed
	buf = []byte{0x12, 0x34, 0x56, 0x78}
	needBits = uint(24) // need 3 bytes, have 4
	expanded = ensureBufferSize(buf, needBits)

	if len(expanded) != 4 {
		t.Errorf("ensureBufferSize changed buffer size to %d, want 4", len(expanded))
	}

	// Check data is unchanged
	if expanded[0] != 0x12 || expanded[1] != 0x34 || expanded[2] != 0x56 || expanded[3] != 0x78 {
		t.Error("ensureBufferSize changed data when no expansion was needed")
	}
}

func TestWriteBitWindow(t *testing.T) {
	buf := make([]byte, 4)

	// Write window at byte 0
	w := uint16(0x1234)
	writeBitWindow(buf, 0, w)

	if buf[0] != 0x12 || buf[1] != 0x34 {
		t.Errorf("writeBitWindow wrote 0x%02x%02x, want 0x1234", buf[0], buf[1])
	}

	// Write window at byte 1
	buf = make([]byte, 4)
	w = uint16(0x5678)
	writeBitWindow(buf, 1, w)

	if buf[1] != 0x56 || buf[2] != 0x78 {
		t.Errorf("writeBitWindow wrote 0x%02x%02x at offset 1, want 0x5678", buf[1], buf[2])
	}
}

// Integration tests to verify the refactored functions work the same as originals

func TestNybIntegration(t *testing.T) {
	// Test data that would be used in a real Merkle tree
	key := []byte{0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0}
	fanout := uint32(16) // 4 bits per level

	// Test reading nybbles at different positions
	expected := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0}

	for i := uint8(0); i < uint8(len(expected)); i++ {
		result := nyb(key, i, fanout)
		if result != expected[i] {
			t.Errorf("nyb(key, %d, %d) = %d, want %d", i, fanout, result, expected[i])
		}
	}
}

func TestWriteDigitIntegration(t *testing.T) {
	// Test writing digits and then reading them back
	buf := make([]byte, 0)
	fanout := uint32(16) // 4 bits per level
	bpl := bitsPerLevel(fanout)

	// Write some test digits
	testDigits := []uint32{1, 2, 3, 4, 5, 6, 7, 8}

	for i, digit := range testDigits {
		buf = writeDigit(buf, uint(i), digit, bpl)
	}

	// Read them back using nyb function
	for i, expected := range testDigits {
		result := nyb(buf, uint8(i), fanout)
		if result != int(expected) {
			t.Errorf("After writeDigit, nyb(buf, %d, %d) = %d, want %d", i, fanout, result, expected)
		}
	}
}

func TestCrossByteOperations(t *testing.T) {
	// Test operations that span across byte boundaries
	buf := make([]byte, 0)
	fanout := uint32(16) // 4 bits per level
	bpl := bitsPerLevel(fanout)

	// Write digits that will span across bytes
	// Position 1: bits 4-7 (second nybble in first byte)
	// Position 2: bits 8-11 (first nybble in second byte)
	buf = writeDigit(buf, 1, 0x0A, bpl) // Write 10 at position 1
	buf = writeDigit(buf, 2, 0x0B, bpl) // Write 11 at position 2

	// Verify the results
	if result := nyb(buf, 1, fanout); result != 0x0A {
		t.Errorf("nyb(buf, 1, %d) = 0x%02x, want 0x0A", fanout, result)
	}
	if result := nyb(buf, 2, fanout); result != 0x0B {
		t.Errorf("nyb(buf, 2, %d) = 0x%02x, want 0x0B", fanout, result)
	}
}
