//! # Python Interface for IEEE C37.118 PDC Data Processing
//!
//! This module provides a Python interface for interacting with a Phasor Data Concentrator
//! (PDC) server using the IEEE C37.118 standard, as defined in IEEE C37.118-2005,
//! IEEE C37.118.2-2011, and IEEE C37.118.2-2024. It wraps the `rtpa_core::pdc_buffer::PDCBuffer`
//! Rust implementation, exposing a Python class to connect to PDC servers, manage data streams,
//! and retrieve timeseries data as PyArrow record batches.
//!
//! ## Key Components
//!
//! - `PDCBuffer`: A Python class for managing PDC connections, configuration, and data retrieval.
//! - `rtpa`: The Python module entry point, registering the `PDCBuffer` class.
//!
//! ## Usage
//!
//! This module is designed for Python applications in power system monitoring, enabling users
//! to stream synchrophasor data from PDC servers and process it using PyArrow for efficient
//! analysis. It integrates with the `rtpa_core` crate for core PDC functionality, leveraging
//! Rust’s performance and Arrow’s data format for interoperability with Python data science
//! ecosystems.

use ::rtpa_core::pdc_buffer::PDCBuffer as PDCBufferRust;
use arrow::pyarrow::ToPyArrow;
use arrow::record_batch::RecordBatch;
use pyo3::prelude::*;

extern crate serde_json;

/// A Python class for interacting with a PDC server over IEEE C37.118.
///
/// Wraps the Rust `PDCBufferRust` to provide Python methods for connecting to a PDC server,
/// managing data streams, and retrieving configuration and timeseries data as PyArrow
/// record batches, suitable for power system monitoring.
///
/// # Fields
///
/// * `inner`: Optional `PDCBufferRust` instance, initialized on `connect`.
#[pyclass]
pub struct PDCBuffer {
    inner: Option<PDCBufferRust>,
}

#[pymethods]
impl PDCBuffer {
    /// Creates a new PDCBuffer instance.
    ///
    /// The buffer is initialized without an active connection to avoid
    /// thread-safety issues. Call `connect()` to establish a connection.
    ///
    /// Returns:
    ///     PDCBuffer: A new PDCBuffer instance.
    #[new]
    fn new() -> Self {
        // Don't create the PDCBuffer right away to avoid thread-safety issues
        PDCBuffer { inner: None }
    }

    /// Connects to a PDC stream with the specified parameters.
    ///
    /// Parameters:
    ///     ip_addr (str): The IP address of the PDC stream.
    ///     port (int): The port number for the connection.
    ///     id_code (int): The ID code for the PDC stream.
    ///     version (str, optional): The IEEE C37.118 version (e.g., "2005", "2011").
    ///         Defaults to None, which uses the default version (2011).
    ///     output_format (str, optional): The phasor output format ("FloatPolar" or "FloatRect").
    ///         Defaults to None, which uses the default format.
    ///     batch_size (int, optional): The size of data batches. Defaults to None.
    ///     max_batches (int, optional): The maximum number of batches to store.
    ///         Defaults to None.
    ///
    /// Raises:
    ///     ValueError: If the version or output_format is invalid.
    ///     RuntimeError: If the connection fails.
    ///
    /// Returns:
    ///     None
    #[pyo3(
        text_signature = "(ip_addr, port, id_code, version=None, output_format=None, batch_size=None, max_batches=None)",
        signature=(ip_addr, port, id_code, version = None, output_format = None, batch_size = None, max_batches = None))]
    fn connect(
        &mut self,
        ip_addr: String,
        port: u16,
        id_code: u16,
        version: Option<String>,
        output_format: Option<String>,
        batch_size: Option<usize>,
        max_batches: Option<usize>,
    ) -> PyResult<()> {
        // Convert version string to Version enum if provided
        let version_enum = match version {
            Some(v) => Some(
                ::rtpa_core::ieee_c37_118::common::Version::from_string(&v)
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
            ),
            None => None,
        };

        // Convert phasor_type string to PhasorType enum if provided, limiting to FloatPolar or FloatRect
        let phasor_type_enum = match output_format {
            Some(pt) => {
                // Validate that only allowed types are specified
                match pt.as_str() {
                    "FloatPolar" | "FloatRect" => Some(
                        ::rtpa_core::ieee_c37_118::phasors::PhasorType::from_str(&pt)
                            .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
                    ),
                    _ => {
                        return Err(pyo3::exceptions::PyValueError::new_err(
                            "phasor_type must be one of: 'FloatPolar', 'FloatRect', or None",
                        ))
                    }
                }
            }

            None => None, // Let Rust implementation use its default
        };

        self.inner = Some(PDCBufferRust::new(
            ip_addr,
            port,
            id_code,
            version_enum,
            batch_size,
            max_batches,
            phasor_type_enum,
        ));
        Ok(())
    }

    /// Retrieves the configuration of the connected PDC as a Python dictionary.
    ///
    /// Parameters:
    ///     py (Python): The Python interpreter instance (injected by PyO3).
    ///
    /// Returns:
    ///     dict: A dictionary containing the PDC configuration.
    ///
    /// Raises:
    ///     RuntimeError: If not connected (call `connect()` first).
    ///     ValueError: If the configuration cannot be serialized to JSON.
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

    /// Starts the PDC data stream.
    ///
    /// Raises:
    ///     RuntimeError: If not connected (call `connect()` first).
    ///
    /// Returns:
    ///     None
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

    /// Stops the PDC data stream.
    ///
    /// Raises:
    ///     RuntimeError: If not connected (call `connect()` first).
    ///
    /// Returns:
    ///     None
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

    /// Lists the PMUs (Phasor Measurement Units) in the PDC stream.
    ///
    /// Returns:
    ///     list[str]: A list of PMU names.
    ///
    /// Raises:
    ///     RuntimeError: If not connected (call `connect()` first).
    fn list_pmus(&self) -> PyResult<Vec<(String, u16)>> {
        match &self.inner {
            Some(buffer) => Ok(buffer.list_pmus()),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Not connected. Call connect() first",
            )),
        }
    }

    /// Sets the PMU filter to include only specified PMUs or all PMUs.
    ///
    /// Stops any active streaming, reconfigures the accumulators to include only the specified
    /// PMUs (by ID codes), and resets the accumulator manager. If `id_codes` is None, includes
    /// all PMUs.
    ///
    /// Parameters:
    ///     id_codes (list[int], optional): List of PMU ID codes to filter accumulators.
    ///         Defaults to None, which includes all PMUs.
    ///     output_format (str, optional): The phasor output format ("FloatPolar" or "FloatRect").
    ///         Defaults to None, which uses the default format.
    ///
    /// Raises:
    ///     RuntimeError: If not connected (call `connect()` first).
    ///     ValueError: If the output_format is invalid or if the filter cannot be applied.
    ///
    /// Returns:
    ///     None
    #[pyo3(
            text_signature = "(id_codes=None, output_format=None)",
            signature=(id_codes = None, output_format = None))]
    fn set_pmu_filter(
        &mut self,
        id_codes: Option<Vec<u16>>,
        output_format: Option<String>,
    ) -> PyResult<()> {
        match &mut self.inner {
            Some(buffer) => {
                let phasor_type_enum = match output_format {
                    Some(pt) => match pt.as_str() {
                        "FloatPolar" | "FloatRect" => Some(
                            ::rtpa_core::ieee_c37_118::phasors::PhasorType::from_str(&pt).map_err(
                                |e| pyo3::exceptions::PyValueError::new_err(e.to_string()),
                            )?,
                        ),
                        _ => {
                            return Err(pyo3::exceptions::PyValueError::new_err(
                                "output_format must be one of: 'FloatPolar', 'FloatRect', or None",
                            ))
                        }
                    },
                    None => None,
                };

                buffer
                    .set_pmu_filter(id_codes, phasor_type_enum)
                    .map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "Failed to set PMU filter: {}",
                            e
                        ))
                    })?;
                Ok(())
            }
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Not connected. Call connect() first",
            )),
        }
    }

    /// Retrieves the latest raw sample from the PDC buffer.
    ///
    /// Returns:
    ///     bytes: The raw sample data.
    ///
    /// Raises:
    ///     RuntimeError: If not connected or if the sample cannot be retrieved.
    fn get_raw_sample(&self) -> PyResult<Vec<u8>> {
        match &self.inner {
            Some(buffer) => buffer
                .get_latest_buffer()
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e)),
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Not connected. Call connect() first",
            )),
        }
    }

    /// Retrieves the location of a specific channel.
    ///
    /// Parameters:
    ///     channel_name (str): The name of the channel.
    ///
    /// Returns:
    ///     tuple[int, int]: A tuple containing the PMU ID and channel index.
    ///
    /// Raises:
    ///     ValueError: If the channel is not found.
    ///     RuntimeError: If not connected (call `connect()` first).
    fn get_channel_location(&self, channel_name: &str) -> PyResult<(u16, u8)> {
        match &self.inner {
            Some(buffer) => match buffer.get_channel_location(channel_name) {
                Some(location) => Ok(location),
                None => Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "Channel '{}' not found",
                    channel_name
                ))),
            },
            None => Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Not connected. Call connect() first",
            )),
        }
    }

    /// Retrieves data from the PDC buffer as a pyarrow RecordBatch.
    ///
    /// Parameters:
    ///     py (Python): The Python interpreter instance (injected by PyO3).
    ///     columns (list[str], optional): A list of column names to include.
    ///         Defaults to None, which includes all columns.
    ///     window_secs (int, optional): The time window in seconds for data retrieval.
    ///         Defaults to None, which uses the default window.
    ///
    /// Returns:
    ///     pyarrow.RecordBatch: A pyarrow RecordBatch containing the requested data.
    ///
    /// Raises:
    ///     RuntimeError: If not connected or if data retrieval fails.
    ///     ValueError: If the RecordBatch cannot be converted to pyarrow.
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

/// A Python module implemented in Rust for interacting with PDC streams.
#[pymodule]
fn rtpa(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PDCBuffer>()?;
    Ok(())
}
