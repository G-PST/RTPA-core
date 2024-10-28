# Prototype for PMU data analytics
> [!CAUTION]
> This software is experimental and subject to change.

## How to run

Clone the entire repo

```console
git clone https://github.com/G-PST/pmu-data-analytics.git
```

Running the CLI:

```console
cargo run --help
```


Reading PMU data from Python

```python
import io
import pandas as pd
import requests

url = "http://127.0.0.1:8080/data"
s = requests.get(url, timeout=10)
df = pd.read_feather(io.BytesIO(requests.get(url, timeout=10))
df.head()
```


Todo:

- [ ] Add handler for broken pipe,
- [ ] Add BSD3 licence,
- [ ] Improve documentation to add examples,
- [ ] Create a more robust mock PDC server with multiple PMU's,
- [ ] Additional HTTP endpoints:
    - [ ] PDC connection status,
    - [ ] Additional HTTP endpoints to get the configuration and header as JSON,
    - [ ] Filter to get the last X elements from the buffer (optional),
- [ ] Add package to cargo,
- [ ] Add more logging on the server to see HTTP request coming in,
- [ ] Add retry logic for server connection to PDC,
- [ ] Support for the IEEE C37.118 2024 or version 3.0,
- [ ] Add support for `ConfigurationFrame3`,
