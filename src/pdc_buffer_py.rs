use pyo3::prelude::*;

#[pyclass]
struct PDCBufferPy {
    inner: PDCBuffer,
}

#[pymethods]
impl PDCBufferPy {
    #[new]
    fn new(ip_addr: String, port: u16) -> Self {
        PDCBufferPy {
            inner: PDCBuffer::new(ip_addr, port),
        }
    }

    fn start_stream(&mut self) {
        self.inner.start_stream();
    }

    fn stop_stream(&mut self) {
        self.inner.stop_stream();
    }

    fn get_pdc_dataframes(&self) -> PyResult<Vec<String>> {
        // Placeholder: Convert AccumulatorManager::get_dataframe to Python-friendly format
        Ok(self.inner.get_pdc_dataframes())
    }
}

#[pymodule]
fn pdc_buffer_module(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PDCBufferPy>()?;
    Ok(())
}
