# Prototype for PMU data analytics
> [!CAUTION]
> This software is experimental and subject to change.


## Getting started

Clone the entire repo

```console
git clone https://github.com/G-PST/pmu-data-analytics.git
```

Running the CLI using
[cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html):

```console
cargo run --help
```

## How to ...

### ... start the Mock PDC server

```console
cargo run mock-pdc
```

### ... start the Mock PDC server in a non-default IP/PORT

```console
cargo run -- mock-pdc --ip localhost --port 8080
```

### ... start the PMU server

```console
cargo run server
```

### ... start the PMU server and connect to a PDC

Assuming that the IP of the PDC server is `160.49.88.18` and the port enable is
`3030`

```console
cargo run -- server --pdc-ip 160.49.88.18 --port 3030
```

### ... change the frequency of the PDC server

```console
cargo run -- server --pdc-ip localhost --port 8080
```

### ... change the HTTP server port of the application

```console
cargo run -- server --http-port 3030
```

### ... read the data from the PMU server in Python

While the server is running you can use Python to access the memory buffer using
pandas:

```python
import io
import pandas as pd
import requests

PORT = 8080  # Port where the Server was bind
url = f"http://127.0.0.1:{PORT}/data"
s = requests.get(url, timeout=10)
df = pd.read_feather(io.BytesIO(requests.get(url, timeout=10).content))
df.head()
```
