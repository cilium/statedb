// +build amd64

#include "textflag.h"

// func findKeyIndexAVX2(keys *[16]byte, key byte) int
TEXT ·findKeyIndexAVX2(SB), NOSPLIT, $0-16
	// Load arguments
	MOVQ keys+0(FP), SI    		// SI = keys array base pointer
	MOVB key+8(FP), AL      	// AL = key byte to find

	// Broadcast target byte to all lanes of XMM0
	MOVBQZX AL, AX  // Move byte from AL to quadword AX and zero-extend
	MOVQ    AX, X0  // Move quadword from AX to X0
	VPBROADCASTB X0, X1

	// Load 16 bytes from keys array (only load what we need)
	VMOVDQU (SI), X2

	// Compare with target byte using VPCMPEQB
	VPCMPEQB X2, X1, X3

	// Get mask of matching bytes (16 bits for 16 bytes)
	VPMOVMSKB X3, AX

	// Check if any match was found (AX == 0 means no match)
	CMPQ AX, $0
	JE no_match

	// Find the first set bit (first matching byte)
	BSFQ AX, AX

	// Return the result
	MOVQ AX, ret+16(FP)
	RET

no_match:
	// No match found, return -1
	MOVQ $-1, ret+16(FP)
	RET

// func findInsertPositionAVX2(keys *[16]byte, key byte) int
TEXT ·findInsertPositionAVX2(SB), NOSPLIT, $0-16
	// Load arguments
	MOVQ keys+0(FP), SI    	// SI = keys array base pointer
	MOVB key+8(FP), AL      // AL = key byte to find

	// Broadcast target byte to all lanes of XMM0
	MOVBQZX AL, AX  // Move byte from AL to quadword AX and zero-extend
	MOVQ    AX, X0  // Move quadword from AX to X0
	VPBROADCASTB X0, X1

	// Load 16 bytes from keys array (only load what we need)
	VMOVDQU (SI), X2

	// Prepare unsigned compare by flipping sign bit (xor with 0x80)
	MOVB $0x80, AL
	MOVBQZX AL, AX
	MOVQ AX, X4
	VPBROADCASTB X4, X4
	VPXOR X4, X1, X1
	VPXOR X4, X2, X2

	// Compare target with keys (unsigned comparison for greater than)
	// X3[i] = 1 if X2[i] > X1[i] (keys[i] > target)
	VPCMPGTB X2, X1, X3

	// Get mask of bytes greater than target (16 bits for 16 bytes)
	VPMOVMSKB X3, AX

	// Check if any byte is greater than target
	CMPQ AX, $0
	JE all_less_equal

	// Find the first set bit (first byte greater than target)
	BSFQ AX, AX

	// Return the result
	MOVQ AX, ret+16(FP)
	RET

all_less_equal:
	// All bytes are less than or equal to target, insert at end
	MOVQ $16, ret+16(FP)
	RET

// func findNonZeroKeyIndexAVX2(keys *[256]byte) int
TEXT ·findNonZeroKeyIndexAVX2(SB), NOSPLIT, $0-8
	// Load arguments
	MOVQ keys+0(FP), SI		// SI = keys array base pointer

	// Initialize result to -1 (no non-zero found)
	MOVQ $-1, AX

	// Process 256 bytes in 32-byte chunks (8 iterations)
	MOVQ $8, CX				// CX = loop counter

loop:
	// Load 32 bytes from keys array
	VMOVDQU (SI), Y0

	// Compare with zero using VPCMPEQB
	VPXOR Y1, Y1, Y1			// Y1 = 0 (zero vector)
	VPCMPEQB Y0, Y1, Y2		// Y2 = (Y0 == 0)

	// Get mask of zero bytes (32 bits for 32 bytes)
	VPMOVMSKB Y2, DX

	// If all bytes are zero (DX == 0xFFFFFFFF), continue to next chunk
	CMPL DX, $0xFFFFFFFF
	JE next_chunk

	// Find first non-zero byte in this chunk
	// Invert the mask to get non-zero bytes
	NOTL DX

	// Find the first set bit (first non-zero byte)
	BSFL DX, DX

	// Calculate global index: (8 - CX) * 32 + DX
	MOVQ $8, R8
	SUBQ CX, R8					// R8 = (8 - CX)
	SHLQ $5, R8					// R8 = (8 - CX) * 32
	ADDQ DX, R8					// R8 = (8 - CX) * 32 + DX

	// Return the result
	MOVQ R8, ret+8(FP)
	RET

next_chunk:
	// Move to next 32-byte chunk
	ADDQ $32, SI
	DECQ CX
	JNZ loop

	// No non-zero bytes found, return -1
	MOVQ $-1, ret+8(FP)
	RET

// func findLastNonZeroKeyIndexAVX2(keys *[256]byte) int
TEXT ·findLastNonZeroKeyIndexAVX2(SB), NOSPLIT, $0-8
	// Load arguments
	MOVQ keys+0(FP), SI		// SI = keys array base pointer

	// Initialize result to -1 (no non-zero found)
	MOVQ $-1, AX

	// Process 256 bytes in 32-byte chunks (8 iterations)
	MOVQ $0, CX				// CX = chunk index (0-7)

loop_forward:
	// Load 32 bytes from keys array
	VMOVDQU (SI), Y0

	// Compare with zero using VPCMPEQB
	VPXOR Y1, Y1, Y1			// Y1 = 0 (zero vector)
	VPCMPEQB Y0, Y1, Y2		// Y2 = (Y0 == 0)

	// Get mask of zero bytes (32 bits for 32 bytes)
	VPMOVMSKB Y2, DX

	// If all bytes are zero (DX == 0xFFFFFFFF), continue to next chunk
	CMPL DX, $0xFFFFFFFF
	JE next_chunk

	// Find last non-zero byte in this chunk
	// Invert the mask to get non-zero bytes
	NOTL DX

	// Find the last set bit (last non-zero byte in this chunk)
	BSRL DX, DX

	// Calculate global index: CX * 32 + DX
	MOVQ CX, R8
	SHLQ $5, R8					// R8 = CX * 32
	ADDQ DX, R8					// R8 = CX * 32 + DX

	// Update result if this index is higher than current result
	CMPQ R8, AX
	CMOVQGT R8, AX

next_chunk:
	// Move to next 32-byte chunk
	ADDQ $32, SI
	INCQ CX
	CMPQ CX, $8
	JL loop_forward

	// Return the result (highest non-zero index found)
	MOVQ AX, ret+8(FP)
	RET
