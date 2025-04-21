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
    Run this script with a running PDC server at the specified IP and port (e.g., 127.0.0.1:8123).
    Ensure the `rtpa` package is installed and the server supports IEEE C37.118.

Copyright and Authorship:
    Copyright (c) 2025 Alliance for Sustainable Energy, LLC.
    Developed by Micah Webb at the National Renewable Energy Laboratory (NREL).
    Licensed under the BSD 3-Clause License. See the `LICENSE` file for details.
"""

from rtpa import PDCBuffer
import pandas as pd
import polars as pl
from time import sleep, time
import binascii  # For hex conversion

# Initialize the PDCBuffer instance
pdc_buffer = PDCBuffer()

# Connect to the PDC server at 127.0.0.1:8123 with ID code 235, using IEEE C37.118-2011
# Output format is set to None to use native phasor formats
pdc_buffer.connect("127.0.0.1", 8123, 235, version="v1", output_format=None)
# Alternative connection (commented out): pdc_buffer.connect("127.0.0.1", 8900, 235)

# Start streaming synchrophasor data from the PDC server
pdc_buffer.start_stream()

print("Stream started, waiting for buffer to fill")
# Wait 15 seconds to allow the buffer to accumulate data
sleep(15)

print("requesting data")
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

print("Data received")
# Print performance metrics for data retrieval and conversions
print(f"Time taken to produce record batch: {(t2-t1)*1000:.1f} milliseconds")
print(f"Time taken to convert to pandas DataFrame: {(t3-t2)*1000:.1f} milliseconds")
print(f"Time taken to convert to datetime: {(t4-t3)*1000:.1f} milliseconds")
print(f"Time taken to convert to polars: {(t5-t4)*1000:.1f} milliseconds")

# Calculate and print the memory usage of the Pandas DataFrame
size_in_bytes = df.memory_usage(deep=True).sum()
size_in_mb = size_in_bytes / (1024 * 1024)
print(f"Size in MB: {size_in_mb:.2f}")

print("Number of rows")
# Print the number of rows in the DataFrame
print(f"Num rows: {len(df)}")

# Print the time range of the data
print("Start and end Time")
print(df['DATETIME'].max(), df['DATETIME'].min())

# Print the last few rows of the DataFrame
print(df.tail())

print()
# Print the second row for inspection
print(df.iloc[1])

print()
# Print the number of columns in the DataFrame
print(f"Num columns: {len(df.columns)}")

# Retrieve and print the PDC configuration (commented out)
# print(pdc_buffer.get_configuration())

# Retrieve and analyze a raw data frame
try:
    # Get the latest raw sample as a byte array
    raw_sample = pdc_buffer.get_raw_sample()
    print("\nRaw sample (first 100 bytes as hex):")
    # Print the first 100 bytes as a hexadecimal string
    print(binascii.hexlify(raw_sample[:100]).decode('utf-8'))

    # Print the first 100 bytes in a formatted hex dump (16 bytes per line)
    hex_bytes = [f"{b:02x}" for b in raw_sample[:100]]
    print("\nRaw sample (formatted):")
    for i in range(0, len(hex_bytes), 16):
        print(" ".join(hex_bytes[i:i+16]))

    # Get the location of a specific channel (TODO: replace with dynamic channel selection)
    channel_name = df.columns[4]
    location_info = pdc_buffer.get_channel_location(channel_name)
    print(f"\nChannel '{channel_name}' location: offset={location_info[0]}, length={location_info[1]} bytes")

    # Print the hex bytes corresponding to the channel’s data
    print(f"\n{channel_name} Values: {hex_bytes[location_info[0]: location_info[0]+location_info[1]]}")
except Exception as e:
    print(f"Error getting raw sample: {e}")

# Stop streaming and close the connection
pdc_buffer.stop_stream()
