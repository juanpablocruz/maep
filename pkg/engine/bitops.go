package engine

import "math/bits"

// BitOps provides utilities for bit-level operations on byte arrays,
// specifically for handling multi-bit digits/nybbles in Merkle tree operations.
//
// These utilities are used by both the Merkle tree implementation (for reading
// nybbles from keys) and the SV (Summary Vector) implementation (for writing
// digits to prefix paths). They handle cross-byte bit operations using 16-bit
// windows to ensure correct behavior when bits span across byte boundaries.
//
// Key concepts:
// - bpl (bits per level): number of bits needed to represent fanout levels
// - bit windows: 16-bit windows used to handle cross-byte operations
// - MSB-first: bits are processed from most significant to least significant

// ensures fanout is a valid power of two
func validateFanout(fanout uint32) {
	if fanout == 0 || fanout&(fanout-1) != 0 {
		panic("fanout must be power of two")
	}
}

// calculates the number of bits needed to represent fanout levels
func bitsPerLevel(fanout uint32) uint {
	validateFanout(fanout)
	return uint(bits.Len32(fanout - 1))
}

// calculates the bit position and byte index for a digit at position pos
func bitPosition(pos uint, bpl uint) (startBit uint, byteIdx uint, off uint) {
	startBit = pos * bpl
	byteIdx = startBit / 8
	off = startBit % 8 // 0 means MSB of the byte
	return
}

// creates a 16-bit window from a byte array at the given byte index
// to handle cross-byte bit operations
func createBitWindow(buf []byte, byteIdx uint) uint16 {
	var w uint16
	if int(byteIdx) < len(buf) {
		w = uint16(buf[byteIdx]) << 8
	}
	if int(byteIdx)+1 < len(buf) {
		w |= uint16(buf[byteIdx+1])
	}
	return w
}

// writes a 16-bit window back to a byte array at the given byte index
func writeBitWindow(buf []byte, byteIdx uint, w uint16) {
	if int(byteIdx) < len(buf) {
		buf[byteIdx] = byte(w >> 8)
	}
	if int(byteIdx)+1 < len(buf) {
		buf[byteIdx+1] = byte(w)
	}
}

// calculates the mask and shift values for bit operations
func calculateMaskAndShift(off uint, bpl uint) (mask uint16, shift uint) {
	shift = 16 - off - bpl
	mask = uint16((1 << bpl) - 1)
	return
}

// ensures the buffer is large enough to hold the required bits
func ensureBufferSize(buf []byte, needBits uint) []byte {
	needBytes := int((needBits + 7) / 8)
	if len(buf) < needBytes {
		nb := make([]byte, needBytes)
		copy(nb, buf)
		return nb
	}
	return buf
}
