# harlequin-databend

This repo provides the Harlequin adapter for [Databend](https://github.com/datafuselabs/databend).

## Installation

`harlequin-databend` depends on `harlequin` and [databend-py](https://github.com/datafuselabs/databend-py), so installing this package will also install these packages.

### Using pip

To install this adapter into an activated virtual environment:
```bash
pip install harlequin-databend
```

### Using poetry

```bash
poetry add harlequin-databend
```

### Using pipx

If you do not already have Harlequin installed:

```bash
pip install harlequin-databend
```

If you would like to add the databend adapter to an existing Harlequin installation:

```bash
pipx inject harlequin harlequin-databend
```

## Usage and Configuration

You can open Harlequin with the Databend adapter by selecting it with the `-a` option and passing a [Databend DSN](https://docs.databend.com/guides/sql-clients/developers/python#step-2-configuring-connection-string-for-databend-py):

```bash
harlequin -a databend "http://root@localhost:8000/db?secure=False&copy_purge=True&debug=True"
```

You can also pass all or parts of the connection string as separate options:

```bash
harlequin -a databend -h localhost -p 8000 -u root --password my-pass -d default
```

Many more options are available; to see the full list, run:

```bash
harlequin --help
```

For more information, see the [Harlequin Docs](https://harlequin.sh/docs/postgres/index).
