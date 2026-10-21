# Local debugging with Databricks Connect

How to run and debug this bundle's Python code from VS Code on your own machine, against the Azure workspace
(`adb-7405618954475016`, target `azure-dev`), including the certificate fix needed on the FBISD network.

## What Databricks Connect does

Databricks Connect (`databricks-connect` in `.venv`) is a Python library that gives local code a Spark session
running on Databricks.

```
Your laptop (VS Code)                          Azure Databricks (serverless)
┌─────────────────────────────┐                ┌──────────────────────────┐
│ main.py runs here           │                │                          │
│   argparse, if/else, loops, │   Spark plan   │ Reads tables, joins,     │
│   breakpoints, variables    │ ─────────────▶ │ filters, aggregates      │
│                             │                │                          │
│ spark.read.table(...)       │ ◀───────────── │ Unity Catalog data       │
│ df.show()                   │    results     │                          │
└─────────────────────────────┘                └──────────────────────────┘
```

Plain Python runs locally, so breakpoints and stepping work as usual. Spark operations are sent to serverless, and
only the results you ask for (`.show()`, `.count()`, `.toPandas()`) come back.

`src/dab/taxis.py` and `src/dab/main.py` import `spark` from `databricks.sdk.runtime`. On Databricks that is the
cluster's own session; locally it opens a Databricks Connect session. The same code runs in both places.

| Code | Debug locally? |
|---|---|
| `src/dab/main.py`, `src/dab/taxis.py` | Yes |
| `tests/` | Yes, from the Testing panel |
| `src/sample_notebook.ipynb` | Yes, cell by cell (fill in `catalog`/`schema`; job widgets don't exist locally) |
| Pipeline code `src/dab_etl/transformations/` | **No.** Deploy and run the pipeline |
| Code inside UDFs | **No.** It runs on Databricks, so local breakpoints never pause there |

## Prerequisites

- The `.venv` is already provisioned by `databricks environments setup-local`: Python 3.12, `databricks-connect`
  18.0, matching serverless environment version 5 (see `pyproject.toml`).
- The Databricks VS Code extension is installed and signed in, with the bundle target set to `azure-dev`.
- On the FBISD network: the certificate fix below.

## Step 1: Fix certificates on the FBISD network

**Skip this and every Python call to Databricks hangs for about 5 minutes and then fails.**

The district firewall (Palo Alto) inspects HTTPS traffic and re-signs every certificate with its own root,
`FBISD_PA_Trust`. Windows trusts that root, so the Databricks CLI, deploys and browsers work. Python doesn't use the
Windows certificate store. Python HTTP calls use the `certifi` bundle, and gRPC (which Spark Connect uses) has its
own list. Neither contains the district root.

Symptoms:

- Debugging `main.py` starts, but no breakpoint is ever hit.
- `import databricks.sdk.runtime` never returns.
- With debug logging on: `databricks.sdk.retries Retrying: cannot connect`, from `get_host_metadata`.
- `requests.get("https://adb-7405618954475016.16.azuredatabricks.net")` raises
  `CERTIFICATE_VERIFY_FAILED: self-signed certificate in certificate chain`.

The SDK's default retry budget is 300 seconds, which is why it looks like nothing is happening.

The fix is a certificate bundle containing the normal public roots plus the district root, with Python and gRPC
pointed at it.

**1a. Build the bundle.** In PowerShell, from the project folder:

```powershell
New-Item -ItemType Directory -Force "$HOME\.certs" | Out-Null
$c = Get-Item Cert:\LocalMachine\Root\D384465DC0909A930F214F8FE65BA47B9B15F250
$pem = "-----BEGIN CERTIFICATE-----`n" + [Convert]::ToBase64String($c.RawData,'InsertLineBreaks') + "`n-----END CERTIFICATE-----`n"
$certifi = & .\.venv\Scripts\python.exe -c "import certifi;print(certifi.where())"
(Get-Content $certifi -Raw) + "`n# FBISD_PA_Trust`n" + $pem | Set-Content "$HOME\.certs\ca-bundle.pem" -Encoding ascii
```

`D384465DC0909A930F214F8FE65BA47B9B15F250` is the thumbprint of `FBISD_PA_Trust` in `LocalMachine\Root`.

**1b. Point Python, gRPC and other tools at it.** These are user environment variables, so they apply to every Python
project on the machine:

```powershell
$b = "$HOME\.certs\ca-bundle.pem"
[Environment]::SetEnvironmentVariable("REQUESTS_CA_BUNDLE", $b, "User")                # requests / Databricks SDK
[Environment]::SetEnvironmentVariable("SSL_CERT_FILE", $b, "User")                     # Python ssl, uv
[Environment]::SetEnvironmentVariable("GRPC_DEFAULT_SSL_ROOTS_FILE_PATH", $b, "User")  # gRPC / Spark Connect
```

**1c. Quit VS Code completely and reopen it.** It reads environment variables only at startup; Reload Window isn't
enough.

**Alternative to 1b and 1c: limit it to this project.** User environment variables apply to every program you
start. The bundle is a superset of Python's default list and only adds a root Windows already trusts, so this is
normally harmless (and it fixes other Python tools on this network). If you'd still rather not change anything
machine-wide, skip 1b and 1c, and add the variables to the `"env"` block of both configurations in
`.vscode/launch.json` (Step 4):

```json
"env": {
    "PYTHONPATH": "${workspaceFolder}/src",
    "REQUESTS_CA_BUNDLE": "${userHome}/.certs/ca-bundle.pem",
    "SSL_CERT_FILE": "${userHome}/.certs/ca-bundle.pem",
    "GRPC_DEFAULT_SSL_ROOTS_FILE_PATH": "${userHome}/.certs/ca-bundle.pem"
}
```

`launch.json` is read on every launch, so no restart is needed. The trade-off:

| | Machine-wide (1b) | Project-only (`launch.json`) |
|---|---|---|
| Debugging this project (F5) | Works | Works |
| Testing panel, `uv run pytest` | Works | Still hangs |
| Databricks Connect in other projects | Works | Still hangs |
| Other Python apps | Use the bundle | Unchanged |

Either way, keep `~\.certs\ca-bundle.pem` where it is. If it's moved or deleted, anything pointed at it fails its
HTTPS calls.

**Check it worked** (machine-wide setup only; with the project-only setup, F5 reaching your breakpoint is the check):

```powershell
.\.venv\Scripts\python.exe -c "import requests; print(requests.get('https://adb-7405618954475016.16.azuredatabricks.net/.well-known/databricks-config').status_code)"
```

This should print `200`.

Notes:

- The bundle still holds every public root, so it also works off the district network.
- `FBISD_PA_Trust` expires on **2027-10-29**. When IT replaces it, look up the new thumbprint and rerun 1a.

To find the inspecting root yourself (for example after it's rotated), print the chain Windows sees:

```powershell
$h = "adb-7405618954475016.16.azuredatabricks.net"
$tcp = New-Object Net.Sockets.TcpClient($h, 443)
$ssl = New-Object Net.Security.SslStream($tcp.GetStream(), $false, { $true })
$ssl.AuthenticateAsClient($h)
$chain = New-Object Security.Cryptography.X509Certificates.X509Chain
$chain.ChainPolicy.RevocationMode = 'NoCheck'
$null = $chain.Build((New-Object Security.Cryptography.X509Certificates.X509Certificate2($ssl.RemoteCertificate)))
$chain.ChainElements | ForEach-Object { $_.Certificate.Subject + "  " + $_.Certificate.Thumbprint }
$tcp.Close()
```

The last line of output is the root. If it isn't a public authority, that root is the one to add.

## Step 2: Select the right Python interpreter

`.vscode/settings.json` defaults to `./.venv/bin/python`, which is the macOS/Linux path. On Windows the interpreter
is `.venv\Scripts\python.exe`.

`Ctrl+Shift+P` → **Python: Select Interpreter** → `.\.venv\Scripts\python.exe` (`Python 3.12.11 ('.venv')`).

If you skip this, Pylance reports `Import "databricks.sdk.runtime" could not be resolved`, and the extension's
Python Environment check fails. The choice is stored per workspace on your machine, so leave `settings.json` as it
is for teammates on macOS/Linux.

## Step 3: Pick compute in the Databricks extension

Databricks sidebar → **Configuration** → **Compute** → **Serverless**.

Serverless matches the installed `databricks-connect` 18.0 (environment version 5). A cluster would need Databricks
Runtime 18.0 to match. The **Python Environment** section should then show Databricks Connect as ready.

## Step 4: Create `.vscode/launch.json`

Run and Debug → **create a launch.json file** → **Python Debugger** → **Python File**, then replace the contents
with:

```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "main.py (Databricks Connect)",
            "type": "debugpy",
            "request": "launch",
            "program": "${workspaceFolder}/src/dab/main.py",
            "args": ["--catalog", "dab_dev", "--schema", "dev_<short_name>_dab"],
            "envFile": "${workspaceFolder}/.databricks/.databricks.env",
            "env": { "PYTHONPATH": "${workspaceFolder}/src" },
            "console": "integratedTerminal",
            "justMyCode": true
        },
        {
            "name": "Current file (Databricks Connect)",
            "type": "debugpy",
            "request": "launch",
            "program": "${file}",
            "envFile": "${workspaceFolder}/.databricks/.databricks.env",
            "env": { "PYTHONPATH": "${workspaceFolder}/src" },
            "console": "integratedTerminal",
            "justMyCode": true
        }
    ]
}
```

- Replace `<short_name>` with the part of your email before `@` (for example `keqingli1129`).
  `dev_<short_name>_dab` is the dev schema the bundle creates; see "Schema names" below.
- `envFile` passes the extension's sign-in and serverless settings to the debugger. The extension rewrites that file
  when it restarts, so keep the extension open and showing ready while you debug.
- If you chose the project-only certificate setup in Step 1, add the three certificate variables to `env` in both
  configurations.

## Step 5: Debug

1. Set a breakpoint by clicking left of a line number, for example on `taxis.find_all_taxis().show(5)` in `main.py`.
2. Pick **main.py (Databricks Connect)** in Run and Debug and press **F5**. The first run takes 20–60 seconds while
   serverless starts.

| Key | Action |
|---|---|
| F5 | Continue |
| F10 | Step over |
| F11 | Step into |
| Shift+F11 | Step out |
| F6 | Pause (shows where it's stuck in **Call Stack**) |
| Shift+F5 | Stop |

DataFrames are lazy, so hovering over one shows only its schema. To see rows, use the **Debug Console**
(`Ctrl+Shift+Y`) while paused:

```python
df.printSchema()
df.limit(5).show()
df.count()
df.limit(20).toPandas()
spark.sql("SELECT current_catalog(), current_schema()").show()
```

Always use `.limit()` before `.toPandas()` or `.collect()`. Without it, the whole table is pulled to your machine.

To debug tests, use the Testing panel (flask icon): right-click a test → **Debug Test**. `tests/conftest.py` creates
the Spark session.

## Schema names

The bundle creates the schema it writes to (`resources/dab.schemas.yml`), and the job and pipeline reference it as
`${resources.schemas.dab_schema.name}`.

| Target | Schema |
|---|---|
| `dev`, `azure-dev` | `dab_dev.dev_<short_name>_dab` |
| `prod`, `azure-prod` | `dab_prod.prod` |

In `mode: development` the bundle prefixes schemas it creates with `dev_<short_name>_`, so each developer gets their
own schema. The schema only exists after a deploy (`databricks bundle deploy -t azure-dev`). `bundle destroy` drops
it along with its tables.

Local runs use your local code. The deployed job uses the wheel from the last deploy, so redeploy after local
changes before running the job in the workspace.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Debug starts, breakpoint never hit, no output | Certificate inspection. Do Step 1, then fully restart VS Code |
| `CERTIFICATE_VERIFY_FAILED: self-signed certificate in certificate chain` | Same as above |
| `Import "databricks.sdk.runtime" could not be resolved` (Pylance) | Wrong interpreter. Do Step 2 |
| `ModuleNotFoundError: dab` | `PYTHONPATH` missing from the launch config, or wrong interpreter |
| `cannot configure default credentials` / metadata-service errors | Databricks extension not signed in or not running |
| `SCHEMA_NOT_FOUND` | Wrong `--schema`, or the bundle hasn't been deployed yet |
| Popup: "Cannot find Java 1.8 or higher" | The Oracle Java VS Code extension, not this project. Disable it for the workspace |
| Breakpoint shown as a hollow grey circle | The file isn't the one being run, or it's library code (set `"justMyCode": false`) |
