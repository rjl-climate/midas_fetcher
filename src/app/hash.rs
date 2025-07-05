//! Efficient MD5 hash type for optimal performance
//!
//! This module provides an optimized MD5 hash type that stores hashes as
//! 16-byte arrays instead of strings, reducing memory usage by ~71% and
//! improving hash map performance through faster comparisons.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::errors::{ManifestError, ManifestResult};

/// Efficient MD5 hash storage using 16-byte array
///
/// This type stores MD5 hashes as their raw 16-byte representation rather than
/// as hex strings, providing significant memory and performance benefits:
///
/// - **Memory**: 16 bytes vs ~56 bytes (71% reduction)
/// - **Performance**: Faster hash map lookups via byte comparison
/// - **Allocation**: Stack allocation vs heap allocation
///
/// The type maintains full compatibility with existing hex string formats
/// through transparent serialization and display formatting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Md5Hash([u8; 16]);

impl Md5Hash {
    /// Create an MD5 hash from a hex string
    ///
    /// # Arguments
    ///
    /// * `hex` - 32-character hexadecimal string (case insensitive)
    ///
    /// # Returns
    ///
    /// `Ok(Md5Hash)` if the string is a valid MD5 hex representation,
    /// `Err(ManifestError::InvalidHash)` otherwise
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use midas_fetcher::app::Md5Hash;
    ///
    /// let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e")?;
    /// let hash_upper = Md5Hash::from_hex("50C9D1C465F3CBFF652BE1509C2E2A4E")?;
    /// assert_eq!(hash, hash_upper);
    /// # Ok::<(), midas_fetcher::errors::ManifestError>(())
    /// ```
    pub fn from_hex(hex: &str) -> ManifestResult<Self> {
        // Validate length and characters
        if hex.len() != 32 {
            return Err(ManifestError::InvalidHash {
                hash: hex.to_string(),
            });
        }

        if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(ManifestError::InvalidHash {
                hash: hex.to_string(),
            });
        }

        // Convert hex string to bytes
        let mut bytes = [0u8; 16];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let hex_pair = std::str::from_utf8(chunk).unwrap(); // Safe: validated above
            bytes[i] = u8::from_str_radix(hex_pair, 16).unwrap(); // Safe: validated above
        }

        Ok(Md5Hash(bytes))
    }

    /// Convert the hash to a hex string representation
    ///
    /// Returns a lowercase 32-character hexadecimal string.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use midas_fetcher::app::Md5Hash;
    ///
    /// let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e")?;
    /// assert_eq!(hash.to_hex(), "50c9d1c465f3cbff652be1509c2e2a4e");
    /// # Ok::<(), midas_fetcher::errors::ManifestError>(())
    /// ```
    pub fn to_hex(&self) -> String {
        use std::fmt::Write;
        self.0.iter().fold(String::with_capacity(32), |mut acc, b| {
            write!(&mut acc, "{:02x}", b).unwrap();
            acc
        })
    }

    /// Get the raw byte array representation
    ///
    /// Returns a reference to the underlying 16-byte array.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use midas_fetcher::app::Md5Hash;
    ///
    /// let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e")?;
    /// let bytes = hash.as_bytes();
    /// assert_eq!(bytes.len(), 16);
    /// # Ok::<(), midas_fetcher::errors::ManifestError>(())
    /// ```
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Create from raw bytes (for internal use)
    ///
    /// # Arguments
    ///
    /// * `bytes` - 16-byte array representing the MD5 hash
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Md5Hash(bytes)
    }
}

impl fmt::Display for Md5Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl FromStr for Md5Hash {
    type Err = ManifestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_hex(s)
    }
}

// Transparent serialization - serialize as hex string for JSON compatibility
impl Serialize for Md5Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Md5Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let hex_string = String::deserialize(deserializer)?;
        Self::from_hex(&hex_string).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_hex_strings() {
        let test_cases = [
            "50c9d1c465f3cbff652be1509c2e2a4e",
            "9734faa872681f96b144f60d29d52011",
            "abcdef1234567890abcdef1234567890",
            "00000000000000000000000000000000",
            "ffffffffffffffffffffffffffffffff",
        ];

        for hex in &test_cases {
            let hash = Md5Hash::from_hex(hex).unwrap();
            assert_eq!(hash.to_hex(), hex.to_lowercase());
        }
    }

    #[test]
    fn test_case_insensitive() {
        let lower = "50c9d1c465f3cbff652be1509c2e2a4e";
        let upper = "50C9D1C465F3CBFF652BE1509C2E2A4E";
        let mixed = "50c9D1C465f3CBFF652be1509c2E2A4e";

        let hash_lower = Md5Hash::from_hex(lower).unwrap();
        let hash_upper = Md5Hash::from_hex(upper).unwrap();
        let hash_mixed = Md5Hash::from_hex(mixed).unwrap();

        assert_eq!(hash_lower, hash_upper);
        assert_eq!(hash_upper, hash_mixed);
        assert_eq!(hash_lower.to_hex(), lower); // Always returns lowercase
    }

    #[test]
    fn test_invalid_hex_strings() {
        let invalid_cases = [
            "",                                  // Empty
            "50c9d1c465f3cbff652be1509c2e2a4",   // Too short
            "50c9d1c465f3cbff652be1509c2e2a4e5", // Too long
            "50c9d1c465f3cbff652be1509c2e2a4g",  // Invalid character
            "50c9d1c465f3cbff652be1509c2e2a4X",  // Invalid character
            "50c9d1c4 65f3cbff652be1509c2e2a4e", // Space
            "50c9d1c4-65f3cbff652be1509c2e2a4e", // Hyphen
        ];

        for hex in &invalid_cases {
            assert!(Md5Hash::from_hex(hex).is_err(), "Should reject: {}", hex);
        }
    }

    #[test]
    fn test_from_str_trait() {
        let hex = "50c9d1c465f3cbff652be1509c2e2a4e";
        let hash: Md5Hash = hex.parse().unwrap();
        assert_eq!(hash.to_hex(), hex);
    }

    #[test]
    fn test_display_trait() {
        let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();
        assert_eq!(format!("{}", hash), "50c9d1c465f3cbff652be1509c2e2a4e");
        assert_eq!(
            format!("{:?}", hash),
            "Md5Hash([80, 201, 209, 196, 101, 243, 203, 255, 101, 43, 225, 80, 156, 46, 42, 78])"
        );
    }

    #[test]
    fn test_equality_and_hashing() {
        let hash1 = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();
        let hash2 = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();
        let hash3 = Md5Hash::from_hex("9734faa872681f96b144f60d29d52011").unwrap();

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);

        // Test that equal hashes have equal hash codes
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher1 = DefaultHasher::new();
        let mut hasher2 = DefaultHasher::new();
        hash1.hash(&mut hasher1);
        hash2.hash(&mut hasher2);
        assert_eq!(hasher1.finish(), hasher2.finish());
    }

    #[test]
    fn test_serialization() {
        let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();

        // Test JSON serialization
        let json = serde_json::to_string(&hash).unwrap();
        assert_eq!(json, "\"50c9d1c465f3cbff652be1509c2e2a4e\"");

        // Test deserialization
        let deserialized: Md5Hash = serde_json::from_str(&json).unwrap();
        assert_eq!(hash, deserialized);
    }

    #[test]
    fn test_as_bytes() {
        let hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();
        let bytes = hash.as_bytes();

        assert_eq!(bytes.len(), 16);
        assert_eq!(bytes[0], 0x50);
        assert_eq!(bytes[1], 0xc9);
        assert_eq!(bytes[15], 0x4e);
    }

    #[test]
    fn test_from_bytes() {
        let bytes = [
            0x50, 0xc9, 0xd1, 0xc4, 0x65, 0xf3, 0xcb, 0xff, 0x65, 0x2b, 0xe1, 0x50, 0x9c, 0x2e,
            0x2a, 0x4e,
        ];

        let hash = Md5Hash::from_bytes(bytes);
        assert_eq!(hash.to_hex(), "50c9d1c465f3cbff652be1509c2e2a4e");
        assert_eq!(hash.as_bytes(), &bytes);
    }

    #[test]
    fn test_memory_efficiency() {
        use std::mem;

        // Verify that Md5Hash is exactly 16 bytes (no overhead)
        assert_eq!(mem::size_of::<Md5Hash>(), 16);

        // Compare with String overhead
        let string_hash = "50c9d1c465f3cbff652be1509c2e2a4e".to_string();
        let md5_hash = Md5Hash::from_hex("50c9d1c465f3cbff652be1509c2e2a4e").unwrap();

        // String has significant overhead (24 bytes + heap allocation)
        assert!(mem::size_of_val(&string_hash) > mem::size_of_val(&md5_hash));
        assert_eq!(mem::size_of_val(&md5_hash), 16);
    }
}
