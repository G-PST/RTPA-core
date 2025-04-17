use std::collections::HashMap;

#[derive(Debug)]
pub enum ColumnData {
    Phasor(Vec<[f32; 2]>),
    FixedPhasor(Vec<[i16; 2]>),
    Analog(Vec<f32>),
    FixedAnalog(Vec<i16>),
    Digital(Vec<u16>),
}

#[derive(Debug)]
pub enum ColumnType {
    Phasor,
    FixedPhasor,
    Analog,
    FixedAnalog,
    Digital,
}
pub struct PMUDataStore {
    pub columns: HashMap<String, ColumnData>,
    pub timestamps: Vec<u64>,
    pub current_index: usize,
    pub capacity: usize,
    pub size: usize, // Track actual number of frames stored
}

impl PMUDataStore {
    pub fn new(num_columns: usize, capacity: usize) -> Self {
        Self {
            columns: HashMap::with_capacity(num_columns),
            timestamps: vec![0; capacity],
            current_index: 0,
            capacity,
            size: 0,
        }
    }
    pub fn add_column(&mut self, name: String, column_type: ColumnType) {
        let data = match column_type {
            ColumnType::Phasor => ColumnData::Phasor(vec![[0.0, 0.0]; self.capacity]),
            ColumnType::FixedPhasor => ColumnData::FixedPhasor(vec![[0, 0]; self.capacity]),
            ColumnType::Analog => ColumnData::Analog(vec![0.0; self.capacity]),
            ColumnType::FixedAnalog => ColumnData::FixedAnalog(vec![0; self.capacity]),
            ColumnType::Digital => ColumnData::Digital(vec![0; self.capacity]),
        };
        self.columns.insert(name, data);
    }

    pub fn add_frame(&mut self, timestamp: u64, frame_data: HashMap<String, PMUValue>) {
        self.timestamps[self.current_index] = timestamp;

        for (name, value) in frame_data {
            if let Some(column) = self.columns.get_mut(&name) {
                match (column, value) {
                    (ColumnData::Phasor(vec), PMUValue::Phasor(v)) => vec[self.current_index] = v,
                    (ColumnData::FixedPhasor(vec), PMUValue::FixedPhasor(v)) => {
                        vec[self.current_index] = v
                    }
                    (ColumnData::Analog(vec), PMUValue::Analog(v)) => vec[self.current_index] = v,
                    (ColumnData::FixedAnalog(vec), PMUValue::FixedAnalog(v)) => {
                        vec[self.current_index] = v
                    }
                    (ColumnData::Digital(vec), PMUValue::Digital(v)) => vec[self.current_index] = v,
                    _ => (), // Mismatched types
                }
            }
        }
        self.current_index = (self.current_index + 1) % self.capacity;
        self.size = self.size.min(self.capacity);
        if self.size < self.capacity {
            self.size += 1;
        }
    }

    pub fn get_last_n_seconds(&self, seconds: u64) -> Option<DataSlice> {
        if self.size == 0 {
            return None;
        }

        // Calculate the last written index
        let last_idx = if self.current_index == 0 {
            self.capacity - 1
        } else {
            self.current_index - 1
        };

        let current_time = self.timestamps[last_idx];
        let target_time = current_time.saturating_sub(seconds * 1000);

        let start_idx = self.find_timestamp_index(target_time)?;

        // Calculate count based on actual data size
        let count = if self.size < self.capacity {
            // Buffer hasn't wrapped yet
            if self.current_index > start_idx {
                self.current_index - start_idx
            } else {
                1 // Single frame case
            }
        } else {
            // Buffer has wrapped
            if self.current_index > start_idx {
                self.current_index - start_idx
            } else {
                self.capacity - start_idx + self.current_index
            }
        };

        Some(DataSlice {
            start_idx,
            count,
            wraps: self.size == self.capacity && self.current_index <= start_idx,
        })
    }

    fn find_timestamp_index(&self, target_time: u64) -> Option<usize> {
        if self.size == 0 {
            return None;
        }

        if self.size == 1 {
            return Some(0);
        }

        let search_range = if self.size < self.capacity {
            // Buffer hasn't wrapped
            (0..self.size).collect::<Vec<_>>()
        } else {
            // Buffer has wrapped
            (self.current_index..self.capacity)
                .chain(0..self.current_index)
                .collect::<Vec<_>>()
        };

        for &idx in search_range.iter().rev() {
            if self.timestamps[idx] <= target_time {
                return Some(idx);
            }
        }

        // If we didn't find a matching timestamp, return the oldest available index
        Some(if self.size < self.capacity {
            0
        } else {
            self.current_index
        })
    }

    pub fn get_column_slice(&self, name: &str, slice: &DataSlice) -> Option<Vec<PMUValue>> {
        let column = self.columns.get(name)?;
        let mut result = Vec::with_capacity(slice.count);

        // Helper closure to handle index calculation
        let get_indices = |start: usize, count: usize| {
            if slice.wraps {
                (start..self.capacity)
                    .chain(0..self.current_index)
                    .take(count)
                    .collect::<Vec<_>>()
            } else {
                (start..start + count).collect::<Vec<_>>()
            }
        };

        let indices = get_indices(slice.start_idx, slice.count);

        for idx in indices {
            match column {
                ColumnData::Phasor(vec) => result.push(PMUValue::Phasor(vec[idx])),
                ColumnData::FixedPhasor(vec) => result.push(PMUValue::FixedPhasor(vec[idx])),
                ColumnData::Analog(vec) => result.push(PMUValue::Analog(vec[idx])),
                ColumnData::FixedAnalog(vec) => result.push(PMUValue::FixedAnalog(vec[idx])),
                ColumnData::Digital(vec) => result.push(PMUValue::Digital(vec[idx])),
            }
        }

        Some(result)
    }
}
#[derive(Debug)]
pub struct DataSlice {
    pub start_idx: usize,
    pub count: usize,
    pub wraps: bool,
}

#[derive(Debug)]
pub enum PMUValue {
    Phasor([f32; 2]),
    FixedPhasor([i16; 2]),
    Analog(f32),
    FixedAnalog(i16),
    Digital(u16),
}
