use super::manager::AccumulatorConfig;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub trait DataManager {
    fn schema(&self) -> Arc<Schema>;
    fn process_buffer<F>(&mut self, writer: F) -> Result<(), String>
    where
        F: FnOnce(&mut Vec<u8>) -> usize;
    fn flush_pending_batch(&mut self);
    fn get_dataframe(
        &mut self,
        columns: Option<Vec<&str>>,
        window_secs: Option<u64>,
    ) -> Result<RecordBatch, String>;
    fn duplicate(&self) -> Self;
}

// Add a factory function to create the appropriate manager based on configuration
pub fn create_manager(
    configs: Vec<AccumulatorConfig>,
    use_threading: bool,
    num_threads: usize,
    max_batches: usize,
    buffer_size: usize,
    batch_size: usize,
) -> Box<dyn DataManager> {
    if use_threading {
        Box::new(super::manager::AccumulatorManager::new_with_params(
            configs,
            num_threads,
            max_batches,
            buffer_size,
            batch_size,
        ))
    } else {
        Box::new(
            super::manager_single::SingleThreadedManager::new_with_params(
                configs,
                max_batches,
                buffer_size,
                batch_size,
            ),
        )
    }
}
