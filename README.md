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

## Running With Docker-Compose

### Using the mock-pdc server (all platforms)

*All data will be identical with the exception of timestamps*

First build the containers
```console
docker-compose build
```

Then run
```console
docker-compose up
```

### Using openPDC (x86/64)
*Data will be randomized and simulate a single PDC/PMU*

Assuming you have build the container using the instructions above. You can download and run the open-pdc server and pmu buffer using the command below.

```console
docker-compose -f docker-compose-openpdc.yml up --no-attach open-pdc
```

### Login to OpenPDC

You can log in to the local [openPDC](http://127.0.0.1:8280) instance and update the configuration. The default username and password are.

**USER**
```
.\admin
```

**PASSWORD**
```
admin
```

You can also install the latest release of openPDC with more features [here](https://github.com/GridProtectionAlliance/openPDC/releases/tag/v2.9.148)


## Running the Application with Cargo


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


## Buidling the application Binary

```console
cargo build --release
```

This will build the application binary in .target/release. For windows, the executable will be ./target/release/pmu.exe

You can add the executable to your path and run the commands similar to the commands above replacing **cargo run** with **pmu**.
```console
cargo run server
``` 
vs 
``` console
pmu server
```
or 
```console
./target/release/pmu mock-pdc
```

