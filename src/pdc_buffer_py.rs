#[allow(unused)]
use super::pdc_buffer::PDCBuffer;
use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;

extern crate serde_json;

#[pyclass]
pub struct PDCBufferPy {
    inner: Option<PDCBuffer>,
}

#[pymethods]
impl PDCBufferPy {
    #[new]
    fn new() -> Self {
        // Don't create the PDCBuffer right away to avoid thread-safety issues
        PDCBufferPy { inner: None }
    }

    // TODO: Allow user to specify the desired version standard. PDCBuffer should fall back to default version (2011) if none is specified.

    #[pyo3(signature=(ip_addr, port, id_code,  batch_size = None, max_batches = None))]
    fn connect(
        &mut self,
        ip_addr: String,
        port: u16,
        id_code: u16,
        batch_size: Option<usize>,
        max_batches: Option<usize>,
    ) -> PyResult<()> {
        self.inner = Some(PDCBuffer::new(
            ip_addr,
            port,
            id_code,
            None,
            batch_size,
            max_batches,
        ));
        Ok(())
    }

    fn get_configuration(&self, py: Python<'_>) -> PyResult<PyObject> {
        match &self.inner {
            Some(buffer) => {
                // Get JSON string from PDCBuffer
                let json_string = buffer.config_to_json().map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "Failed to serialize configuration: {}",
                        e
                    ))
                })?;

                // Parse JSON string to Python dictionary
                let json_module = py.import("json")?;
                let py_obj = json_module.call_method1("loads", (json_string,))?;

                Ok(py_obj.into())
            }
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Not connected. Call connect() first",
            )),
        }
    }

    fn start_stream(&mut self) -> PyResult<()> {
        match &mut self.inner {
            Some(buffer) => {
                buffer.start_stream();
                Ok(())
            }
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Not connected. Call connect() first",
            )),
        }
    }

    fn stop_stream(&mut self) -> PyResult<()> {
        match &mut self.inner {
            Some(buffer) => {
                buffer.stop_stream();
                Ok(())
            }
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Not connected. Call connect() first",
            )),
        }
    }

    fn list_pmus(&self) -> PyResult<Vec<String>> {
        match &self.inner {
            Some(buffer) => Ok(buffer.list_pmus()),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Not connected. Call connect() first",
            )),
        }
    }

    fn list_channels(&self) -> PyResult<Vec<String>> {
        match &self.inner {
            Some(buffer) => Ok(buffer.list_channels()),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Not connected. Call connect() first",
            )),
        }
    }

    // In PDCBufferPy
    #[pyo3(signature = (columns=None, window_secs=None))]
    fn get_data(
        &self,
        py: Python<'_>,
        columns: Option<Vec<String>>,
        window_secs: Option<u64>,
    ) -> PyResult<PyObject> {
        match &self.inner {
            Some(buffer) => {
                // Create columns_ref without moving cols prematurely
                let columns_ref = columns
                    .as_ref()
                    .map(|cols| cols.iter().map(|s| s.as_str()).collect::<Vec<&str>>());

                // Call the Rust get_data method
                let record_batch: RecordBatch =
                    buffer.get_data(columns_ref, window_secs).map_err(|e| {
                        pyo3::exceptions::PyRuntimeError::new_err(format!(
                            "Failed to get data: {}",
                            e
                        ))
                    })?;

                // Convert RecordBatch to pyarrow.RecordBatch using ToPyArrow
                let py_record_batch = record_batch.to_pyarrow(py).map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "Failed to convert RecordBatch to pyarrow: {}",
                        e
                    ))
                })?;

                Ok(py_record_batch)
            }
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Not connected. Call connect() first",
            )),
        }
    }
}

#[pymodule]
fn rtpa_core(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PDCBufferPy>()?;
    Ok(())
}
