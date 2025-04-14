# Dependencies

Install maturin to build the wheel.
`pip install maturin[patchelf]`

Run the following command to build the wheel and install it in your local environment:
`maturin develop`

or
`maturin develop --release`


To just build the wheel and save it to the target/wheels directory:
`maturin build --release`

## Test locally

run a python interactive shell with the openpdc server.

`from rtpa_core import PDCBufferPy`

initialize the object
`pdc_buf = PDCBufferPy()`

Connect to the pdc server.
`pdc_buf.connect(ip_addr="127.0.0.1", port=8900, id_code=7734)`

Print the configuration.
`pdc.get_configuration()`

Start streaming
`pdc_buf.start_stream()`

Stop streaming
`pdc_buf.stop_stream()`

Query Data.
