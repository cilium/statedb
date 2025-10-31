//go:build !amd64
// +build !amd64

// Package simd provides SIMD-optimized functions for the ART tree implementation.
// This file contains fallback implementations for architectures that don't support
// AMD64 SIMD instructions, ensuring compatibility across all platforms.
//
// The functions in this package provide scalar implementations that work on
// all architectures, including ARM, ARM64, RISC-V, and others. While these
// implementations are slower than SIMD versions, they ensure that the ART tree
// works correctly on all platforms.
//
// Architecture Support:
//   - All non-AMD64 architectures
//   - ARM (32-bit and 64-bit)
//   - RISC-V (32-bit and 64-bit)
//   - MIPS and other architectures
//
// Performance Characteristics:
//   - Slower than SIMD implementations (typically 4-16x)
//   - Consistent performance across all architectures
//   - No special instruction set requirements
//   - Portable and maintainable code
package part

// FindKeyIndex searches for a key byte in a sorted array using scalar operations.
//
// This function provides a portable implementation that works on all architectures.
//
// Parameters:
//   - keys: Pointer to a 16-byte array of sorted keys
//   - n: The number of valid keys in the array (must be ≤ 16)
//   - key: The key byte to search for
//
// Returns:
//   - Index of the found key (0 to n-1) if found
//   - -1 if the key is not found or n is invalid
//
// Algorithm:
//   - Linear search through the sorted keys array
//   - Early termination on match
//   - Bounds checking for safety
//
// Performance:
//   - Time complexity: O(n) where n is the number of children
//   - Space complexity: O(1)
//   - Portable across all architectures
func FindKeyIndex(keys *[16]byte, n int, key byte) int {
	return findKeyIndexScalar(keys, n, key)
}

// FindInsertPosition finds the insertion position for a key in a sorted array.
//
// This function is used during node insertion to maintain sorted key order.
//
// Parameters:
//   - keys: Pointer to a 16-byte array of sorted keys
//   - n: The number of valid keys in the array (must be ≤ 16)
//   - key: The key byte to find insertion position for
//
// Returns:
//   - Index where the key should be inserted to maintain sorted order
//   - n if the key should be inserted at the end
//
// Algorithm:
//   - Linear search through the sorted keys array
//   - Find first key greater than the target
//   - Return position for maintaining sorted order
//
// Performance:
//   - Time complexity: O(n) where n is the number of children
//   - Space complexity: O(1)
//   - Portable across all architectures
func FindInsertPosition(keys *[16]byte, n int, key byte) int {
	return findInsertPositionScalar(keys, n, key)
}

// FindNonZeroKeyIndex finds the first non-zero key in a 256-byte array.
//
// This function is used by Node48 and Node256 for finding minimum keys.
//
// Parameters:
//   - keys: Pointer to a 256-byte array of sparse keys
//
// Returns:
//   - Index of the first non-zero key (0-255) if found
//   - -1 if all keys are zero
//
// Algorithm:
//   - Linear search from index 0 to 255
//   - Early termination on first non-zero key
//   - Returns -1 if no non-zero keys found
//
// Performance:
//   - Time complexity: O(1) in best case, O(256) in worst case
//   - Space complexity: O(1)
//   - Portable across all architectures
func FindNonZeroKeyIndex(keys *[256]byte) int {
	return findNonZeroKeyIndexScalar(keys)
}

// FindLastNonZeroKeyIndex finds the last non-zero key in a 256-byte array.
//
// This function is used by Node48 and Node256 for finding maximum keys.
//
// Parameters:
//   - keys: Pointer to a 256-byte array of sparse keys
//
// Returns:
//   - Index of the last non-zero key (0-255) if found
//   - -1 if all keys are zero
//
// Algorithm:
//   - Linear search from index 255 down to 0
//   - Early termination on first non-zero key (from end)
//   - Returns -1 if no non-zero keys found
//
// Performance:
//   - Time complexity: O(1) in best case, O(256) in worst case
//   - Space complexity: O(1)
//   - Portable across all architectures
func FindLastNonZeroKeyIndex(keys *[256]byte) int {
	return findLastNonZeroKeyIndexScalar(keys)
}
