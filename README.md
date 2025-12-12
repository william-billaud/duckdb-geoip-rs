# DuckDB geoip extension

This extension export 4 function using MaxminDB database:
* geoip_asn_org(ip : VARCHAR)-> VARCHAR
* geoip_asn_num(ip : VARCHAR)-> VARCHAR
* geoip_city(ip : VARCHAR)-> VARCHAR
* geoip_country_iso(ip : VARCHAR) -> VARCHAR

All the function will return an empty value on empty/non found value.

## Usage

Path to a directory containing `GeoLite2-City.mmdb` and `GeoLite2-ASN.mmdb` files must be exported to `MAXMIND_MMDB_DIR` environment variable. Defaulting to `/usr/share/GeoIP`. 
```
export MAXMIND_MMDB_DIR="`pwd`"
```
Then download extension from the release tab.
To run the extension code, start `duckdb` with `-unsigned` flag. This will allow you to load the local extension file.
```bash
duckdb -unsigned
load '/path/to/extension/duckdb_geoip_rs.duckdb_extension';
```
This extension depends on the [inet](https://duckdb.org/docs/stable/core_extensions/inet) core extensions, which will be automatically installed, except if you do not have access to the internet.
And enjoy

```sql
CREATE TABLE ip_list (ip VARCHAR);
INSERT INTO ip_list VALUES ('1.1.1.1'), ('8.8.8.8'), ('80.8.8.8'), ('90.9.250.1'), ('not_anip');
SELECT ip, geoip_asn_org(ip),geoip_asn_num(ip),geoip_city(ip), geoip_country_iso(ip) from ip_list;
````

```text
┌────────────┬───────────────────┬───────────────────┬────────────────┬───────────────────────┐
│     ip     │ geoip_asn_org(ip) │ geoip_asn_num(ip) │ geoip_city(ip) │ geoip_country_iso(ip) │
│  varchar   │      varchar      │      varchar      │    varchar     │        varchar        │
├────────────┼───────────────────┼───────────────────┼────────────────┼───────────────────────┤
│ 1.1.1.1    │ CLOUDFLARENET     │ 13335             │                │                       │
│ 8.8.8.8    │ GOOGLE            │ 15169             │                │ US                    │
│ 80.8.8.8   │ Orange            │ 3215              │                │ RE                    │
│ 90.9.250.1 │ Orange            │ 3215              │ Lyon           │ FR                    │
│ not_anip   │                   │                   │                │                       │
└────────────┴───────────────────┴───────────────────┴────────────────┴───────────────────────┘
```

## Cloning

Clone the repo with submodules

```shell
git clone --recurse-submodules https://github.com/william-billaud/duckdb-geoip-rs.git
cargo build --release
```

Then loading extension
```
duckdb -unsigned
load './build/release/duckdb_geoip_rs.duckdb_extension';
```


## Dependencies
In principle, these extensions can be compiled with the Rust toolchain alone. However, this template relies on some additional
tooling to make life a little easier and to be able to share CI/CD infrastructure with extension templates for other languages:

- Python3
- Python3-venv
- [Make](https://www.gnu.org/software/make)
- Git

Installing these dependencies will vary per platform:
- For Linux, these come generally pre-installed or are available through the distro-specific package manager.
- For MacOS, [homebrew](https://formulae.brew.sh/).
- For Windows, [chocolatey](https://community.chocolatey.org/).

## Building
After installing the dependencies, building is a two-step process. Firstly run:
```shell
make configure
```
This will ensure a Python venv is set up with DuckDB and DuckDB's test runner installed. Additionally, depending on configuration,
DuckDB will be used to determine the correct platform for which you are compiling.

Then, to build the extension run:
```shell
make debug
```
This delegates the build process to cargo, which will produce a shared library in `target/debug/<shared_lib_name>`. After this step,
a script is run to transform the shared library into a loadable extension by appending a binary footer. The resulting extension is written
to the `build/debug` directory.

To create optimized release binaries, simply run `make release` instead.

## Testing
This extension uses the DuckDB Python client for testing. This should be automatically installed in the `make configure` step.
The tests themselves are written in the SQLLogicTest format, just like most of DuckDB's tests. A sample test can be found in
`test/sql/<extension_name>.test`. To run the tests using the *debug* build:

```shell
make test_debug
```

or for the *release* build:
```shell
make test_release
```

### Version switching
Testing with different DuckDB versions is really simple:

First, run
```
make clean_all
```
to ensure the previous `make configure` step is deleted.

Then, run
```
DUCKDB_TEST_VERSION=v1.3.2 make configure
```
to select a different duckdb version to test with

Finally, build and test with
```
make debug
make test_debug
```

### Known issues
This is a bit of a footgun, but the extensions produced by this template may (or may not) be broken on windows on python3.11
with the following error on extension load:
```shell
IO Error: Extension '<name>.duckdb_extension' could not be loaded: The specified module could not be found
```
This was resolved by using python 3.12
