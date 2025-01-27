use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct Range {
    pub start: usize,
    pub end: usize,
}

impl Range {
    pub fn new(start: usize, end: usize) -> Self {
        assert!(start <= end, "Start must be less than or equal to End");
        Self { start, end }
    }

    pub fn new_full() -> Self {
        Self::from_fraction(1, 1)
    }

    pub fn get_start(&self) -> usize {
        self.start
    }

    pub fn get_end(&self) -> usize {
        self.end
    }

    pub fn is_in_range(&self, value: usize) -> bool {
        value >= self.start && value <= self.end
    }

    pub fn cmp(&self, other: &Self) -> bool {
        self.start == other.start && self.end == other.end
    }

    /// Creates a new murmur3 `Range` based on the segment specified by the fraction `index/total`.
    pub fn from_fraction(index: usize, total: usize) -> Self {
        assert!(
            index > 0 && index <= total,
            "Index must be between 1 and total (inclusive)"
        );
        assert!(total > 0, "Total must be greater than 0");

        let global_start = 0;
        let global_end = u32::MAX as usize;

        let range_size = (global_end - global_start + 1) / total;
        let start = global_start + (index - 1) * range_size;
        // Ensure the last range ends at the maximum possible value
        let end = if index == total {
            global_end
        } else {
            start + range_size - 1
        };

        Self::new(start, end)
    }

    pub fn new_nonexistent() -> Self {
        Self::new(u32::MAX as usize, u32::MAX as usize)
    }

    pub fn is_nonexistent(&self) -> bool {
        self.start == u32::MAX as usize && self.end == u32::MAX as usize
    }
}
