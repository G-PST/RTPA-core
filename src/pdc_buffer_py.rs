use arrow::record_batch::RecordBatch;
use polars::prelude::*;
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass]
struct PDCBufferPy {
    inner: PDCBuffer,
}

#[pymethods]
impl PDCBufferPy {
    #[new]
    fn new(ip_addr: String, port: u16, id_code: u16) -> Self {
        PDCBufferPy {
            inner: PDCBuffer::new(ip_addr, port, id_code, None),
        }
    }

    fn start_stream(&mut self) {
        self.inner.start_stream();
    }

    fn stop_stream(&mut self) {
        self.inner.stop_stream();
    }

    fn get_pdc_configuration(&self) -> PyResult<String> {
        // Assuming ConfigurationFrame has a to_json() method
        // Modify this based on your actual ConfigurationFrame implementation
        let config_json = self
            .inner
            ._config_frame
            .to_json()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{:?}", e)))?;
        Ok(config_json)
    }

    fn get_pdc_dataframes(&self, py: Python) -> PyResult<PyObject> {
        // Get the RecordBatch from the inner implementation
        let record_batch = self
            .inner
            .get_pdc_dataframes()
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(e))?;

        // Convert RecordBatch to Polars DataFrame
        let df = DataFrame::try_from(record_batch)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{:?}", e)))?;

        // Convert Polars DataFrame to Python object
        let py_df = df
            .to_python(py)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("{:?}", e)))?;

        Ok(py_df)
    }

    fn list_pmus(&self) -> PyResult<Vec<String>> {
        Ok(self.inner.list_pmus())
    }

    fn list_channels(&self) -> PyResult<Vec<String>> {
        Ok(self.inner.list_channels())
    }
}

#[pymodule]
fn pdc_buffer_module(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PDCBufferPy>()?;
    Ok(())
}
