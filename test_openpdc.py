from rtpa import PDCBuffer
import pandas as pd
from time import sleep, time
import binascii  # For hex conversion

pdc_buffer = PDCBuffer()

#pdc_buffer.connect("127.0.0.1", 8123, 235)
pdc_buffer.connect("127.0.0.1", 8900, 235)


pdc_buffer.start_stream()

print("Stream started, waiting for buffer to fill")
sleep(5)
print("requesting data")
t1 = time()
record_batch = pdc_buffer.get_data()
t2 = time()
df = record_batch.to_pandas()
t3 = time()
df['DATETIME'] = pd.to_datetime(df['DATETIME'])
t4 = time()

print("Data received")
print(f"Time taken to receive data: {(t2-t1)*1000:.1f} milliseconds")
print(f"Time taken to convert to pandas DataFrame: {(t3-t2)*1000:.1f} milliseconds")
print(f"Time taken to convert to datetime: {(t4-t3)*1000:.1f} milliseconds")

print(df.head())

print()
print(df.iloc[1])

print()
print(pdc_buffer.get_configuration())



# Get and print raw sample as hex
try:
    raw_sample = pdc_buffer.get_raw_sample()
    print("\nRaw sample (first 100 bytes as hex):")
    print(binascii.hexlify(raw_sample[:100]).decode('utf-8'))

    # You can also print in a more readable format with spaces
    hex_bytes = [f"{b:02x}" for b in raw_sample[:100]]
    print("\nRaw sample (formatted):")
    for i in range(0, len(hex_bytes), 16):
        print(" ".join(hex_bytes[i:i+16]))

    # Get location of a specific channel
    channel_name = "SHELBY_2_Lagoon Creek +SI_magnitude (A)"
    location_info = pdc_buffer.get_channel_location(channel_name)
    print(f"\nChannel '{channel_name}' location: offset={location_info[0]}, length={location_info[1]} bytes")

    print(f"\n{channel_name} Values: {hex_bytes[location_info[0]: location_info[0]+location_info[1]]}")
except Exception as e:
    print(f"Error getting raw sample: {e}")


pdc_buffer.stop_stream()
