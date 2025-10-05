
extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

mod geoip;
use crate::geoip::GeoIPASNDatabase;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{BindInfo, InitInfo, TableFunctionInfo, VTab},
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use std::{
    error::Error,
    ffi::CString,
    sync::atomic::{AtomicBool, Ordering},
};

#[repr(C)]
struct GeoIPBindData {
    value: String,
}

#[repr(C)]
struct GeoIPInitData {
    done: AtomicBool,
    asn_db : GeoIPASNDatabase
}

struct GeoIPASNHelloVTab;

impl VTab for GeoIPASNHelloVTab {
    type InitData = GeoIPInitData;
    type BindData = GeoIPBindData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        bind.add_result_column("column0", LogicalTypeHandle::from(LogicalTypeId::Varchar));
        let value = bind.get_parameter(0).to_string();
        Ok(GeoIPBindData { value })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        Ok(GeoIPInitData {
            done: AtomicBool::new(false),
            asn_db: GeoIPASNDatabase::new(),
        })
    }

    fn func(func: &TableFunctionInfo<Self>, output: &mut DataChunkHandle) -> Result<(), Box<dyn std::error::Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();
        if init_data.done.swap(true, Ordering::Relaxed) {
            output.set_len(0);
        } else {
            let vector = output.flat_vector(0);
            let result = CString::new(format!("Rusty Quack {} 🐥", bind_data.value))?;
            vector.insert(0, result);
            output.set_len(1);
        }
        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<GeoIPASNHelloVTab>("geoip_asn")
        .expect("Failed to register hello table function");
    Ok(())
}