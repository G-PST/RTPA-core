# SPDX-License-Identifier: BSD-3-Clause
"""
Demonstrates the usage of the `PDCBuffer` class from the `rtpa` module to stream and
process IEEE C37.118 synchrophasor data from a Phasor Data Concentrator (PDC) server.

This script connects to a PDC server, streams data, retrieves it as a PyArrow RecordBatch,
converts it to Pandas and Polars DataFrames, and analyzes raw samples and channel locations.
It measures performance metrics (e.g., data retrieval, conversion times, memory usage) and
is suitable for power system monitoring applications.

Key Features:
- Connects to a PDC server using IEEE C37.118-2011 (version "v1").
- Streams synchrophasor data and processes it into timeseries DataFrames.
- Retrieves raw data frames and channel metadata for low-level analysis.
- Demonstrates integration with PyArrow, Pandas, and Polars for data processing.

Usage:
    Run this script with a running PDC server at the specified IP and port.
    By default, it connects to 127.0.0.1:8900 (openPDC defaults).
    You can override the host and port using command-line arguments:
    python test_openpdc.py --host <ip_address> --port <port_number>
    Ensure the `rtpa` package is installed and the server supports IEEE C37.118.

Copyright and Authorship:
    Copyright (c) 2025 Alliance for Sustainable Energy, LLC.
    Developed by Micah Webb at the National Renewable Energy Laboratory (NREL).
    Licensed under the BSD 3-Clause License. See the `LICENSE` file for details.
"""

import argparse
from rtpa import PDCBuffer
import pandas as pd
import polars as pl
from time import sleep, time
import binascii  # For hex conversion
from datetime import datetime

# Set up argument parser for host and port
parser = argparse.ArgumentParser(description="Connect to a PDC server and process synchrophasor data.")
parser.add_argument('--host', type=str, default="127.0.0.1", help="IP address of the PDC server (default: 127.0.0.1)")
parser.add_argument('--port', type=int, default=8900, help="Port of the PDC server (default: 8900 for openPDC)")
args = parser.parse_args()

# Initialize the PDCBuffer instance
pdc_buffer = PDCBuffer()

# Connect to the PDC server with the provided or default host and port, using ID code 235 and IEEE C37.118-2011
# Output format is set to None to use native phasor formats
pdc_buffer.connect(args.host, args.port, 235, version="v1", output_format=None)

# Start streaming synchrophasor data from the PDC server
pdc_buffer.start_stream()

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Stream started, waiting for buffer to fill")
# Wait 15 seconds to allow the buffer to accumulate data
sleep(15)

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Requesting data")
# Measure time to retrieve the RecordBatch
t1 = time()
record_batch = pdc_buffer.get_data()
t2 = time()

# Convert the RecordBatch to a Pandas DataFrame
df = record_batch.to_pandas()
t3 = time()

# Convert the DATETIME column to Pandas datetime format
df['DATETIME'] = pd.to_datetime(df['DATETIME'])
t4 = time()

# Convert the RecordBatch to a Polars DataFrame
dfpl = pl.from_arrow(record_batch)
t5 = time()

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Data received")
# Print performance metrics for data retrieval and conversions
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Time taken to produce record batch: {(t2-t1)*1000:.1f} milliseconds")
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Time taken to convert to pandas DataFrame: {(t3-t2)*1000:.1f} milliseconds")
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Time taken to convert to datetime: {(t4-t3)*1000:.1f} milliseconds")
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Time taken to convert to polars: {(t5-t4)*1000:.1f} milliseconds")

# ... (rest of the script remains the same for data processing and output)
# Calculate and print the memory usage of the Pandas DataFrame
size_in_bytes = df.memory_usage(deep=True).sum()
size_in_mb = size_in_bytes / (1024 * 1024)
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Size in MB: {size_in_mb:.2f}")

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Number of rows")
# Print the number of rows in the DataFrame
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Num rows: {len(df)}")

# ... (rest of the script for further processing and closing connection remains unchanged)
pdc_buffer.stop_stream()
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}] Stream stopped")
