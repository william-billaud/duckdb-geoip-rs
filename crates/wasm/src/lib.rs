use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
use duckdb_neo::{Result, extension::Extension};

#[duckdb_entrypoint_c_api_v2(extension_name = "duckdb_geoip_rs_core", api_version = "2.0.0")]
pub fn entrypoint(extension: &Extension) -> Result<()> {
    duckdb_geoip_rs_core::register(extension)
}
