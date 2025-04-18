from rtpa_python import PDCBufferPy
import pandas as pd
from time import sleep
import binascii  # For hex conversion

pdc_buffer = PDCBufferPy()

pdc_buffer.connect("127.0.0.1", 8123, 235)
#pdc_buffer.connect("127.0.0.1", 8900, 235)


pdc_buffer.start_stream()

print("Stream started, waiting for buffer to fill")
sleep(5)
print("requesting data")
record_batch = pdc_buffer.get_data()

df = record_batch.to_pandas()
df['DATETIME'] = pd.to_datetime(df['DATETIME'])
print("Data received")


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
    channel_name = "SHELBY_2_Lagoon Creek +SI_magnitude"
    location_info = pdc_buffer.get_channel_location(channel_name)
    print(f"\nChannel '{channel_name}' location: offset={location_info[0]}, length={location_info[1]} bytes")

    print(f"\n{channel_name} Values: {hex_bytes[location_info[0]: location_info[0]+location_info[1]]}")
except Exception as e:
    print(f"Error getting raw sample: {e}")


pdc_buffer.stop_stream()
