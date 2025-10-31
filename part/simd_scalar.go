package part

// findKeyIndexScalar is a scalar fallback implementation for finding key index.
//
// This function is used by all architectures when SIMD is not available.
// It provides a simple linear search through a sorted array of keys,
// which is efficient for small arrays (≤16 elements) and provides
// consistent performance across all platforms.
//
// Parameters:
//   - keys: Pointer to a 16-byte array of sorted keys
//   - n: The number of valid keys in the array (must be ≤ 16)
//   - key: The key byte to search for
//
// Returns:
//   - Index of the found key (0 to n-1) if found
//   - -1 if the key is not found
//
// Algorithm:
//   - Linear search through the sorted keys array
//   - Early termination on match
//   - Simple and reliable implementation
//
// Performance:
//   - Time complexity: O(n) where n is the number of children
//   - Space complexity: O(1)
//   - Cache-friendly for small arrays
//   - No SIMD overhead or complexity
//
// Use Cases:
//   - Fallback when SIMD is not available
//   - Small arrays where SIMD overhead is not justified
//   - Debugging and testing scenarios
//   - Architectures without SIMD support
func findKeyIndexScalar(keys *[16]byte, n int, key byte) int {
	for i := 0; i < n; i++ {
		if keys[i] == key {
			return i
		}
	}

	return -1
}

// FindInsertPosition is a scalar fallback implementation for finding insert position.
//
// This function is used by all architectures when SIMD is not available.
// It finds the correct insertion position in a sorted array to maintain
// the sorted order invariant required by the ART tree implementation.
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
//   - Optimized for maintaining sorted order
//   - No SIMD overhead or complexity
//
// Use Cases:
//   - Fallback when SIMD is not available
//   - Node insertion operations
//   - Maintaining sorted key order
//   - Architectures without SIMD support
func findInsertPositionScalar(keys *[16]byte, n int, key byte) int {
	for i := 0; i < n; i++ {
		if key < keys[i] {
			return i
		}
	}

	return n
}

// findNonZeroKeyIndexScalar finds the first non-zero key in a 256-byte array.
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
//   - Simple and reliable implementation
//   - No SIMD overhead or complexity
//
// Use Cases:
//   - Finding minimum keys in sparse arrays
//   - Node48 and Node256 minimum operations
//   - Fallback when SIMD is not available
//   - Architectures without SIMD support
func findNonZeroKeyIndexScalar(keys *[256]byte) int {
	for i := 0; i < 256; i++ {
		if keys[i] != 0 {
			return i
		}
	}

	return -1
}

// findLastNonZeroKeyIndexScalar finds the last non-zero key in a 256-byte array.
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
//   - Optimized for finding last non-zero entry
//   - No SIMD overhead or complexity
//
// Use Cases:
//   - Finding maximum keys in sparse arrays
//   - Node48 and Node256 maximum operations
//   - Fallback when SIMD is not available
//   - Architectures without SIMD support
func findLastNonZeroKeyIndexScalar(keys *[256]byte) int {
	for i := 255; i >= 0; i-- {
		if keys[i] != 0 {
			return i
		}
	}

	return -1
}
