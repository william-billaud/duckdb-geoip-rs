extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;
use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeId},
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use libduckdb_sys as ffi;
use maxminddb::geoip2;
use maxminddb::Mmap;
use std::env;
use std::error::Error;
use std::path::Path;
use std::sync::Arc;
pub fn duckdb_string_to_string(word: &ffi::duckdb_string_t) -> String {
    unsafe {
        let len = ffi::duckdb_string_t_length(*word);
        let c_ptr = ffi::duckdb_string_t_data(word as *const _ as *mut _);
        let bytes = std::slice::from_raw_parts(c_ptr as *const u8, len as usize);
        String::from_utf8_lossy(bytes).into_owned()
    }
}

//

pub struct GeoipASNState {
    reader: Arc<maxminddb::Reader<Mmap>>, // Use Arc so it can be shared across function calls safely
}

impl Default for GeoipASNState {
    fn default() -> Self {
        let dbpath_s =
            env::var("MAXMIND_MMDB_DIR").unwrap_or_else(|_| "/usr/share/GeoIP".to_string());
        let dbpath = Path::new(&dbpath_s);
        let reader = maxminddb::Reader::open_mmap(dbpath.join("GeoLite2-ASN.mmdb")).expect(
            format!(
                "Could not load mmdb file, trying to read {}. Use MAXMIND_MMDB_DIR",
                dbpath.join("GeoLite2-ASN.mmdb").display()
            )
            .as_str(),
        );
        Self {
            reader: Arc::new(reader),
        }
    }
}

pub fn invoke_wrapper(
    reader: Arc<maxminddb::Reader<Mmap>>,
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
    geoip_func: fn(db: &maxminddb::Reader<Mmap>, ip: &ffi::duckdb_string_t) -> Option<String>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let input_vector = input.flat_vector(0);
    let sliced_input_vector: &[ffi::duckdb_string_t] = input_vector.as_slice();
    let output_vector = output.flat_vector();

    let count = input.len();
    for i in 0..count {
        if input_vector.row_is_null(i as u64) {
            output_vector.insert(i, "");
            continue;
        }
        let input_str = sliced_input_vector.get(i);
        match { input_str } {
            None => {
                output_vector.insert(i, "");
            }
            Some(s) => {
                output_vector.insert(
                    i,
                    geoip_func(&(reader), s)
                        .unwrap_or_else(|| "".to_string())
                        .as_str(),
                );
            }
        }
    }
    Ok(())
}

pub struct GeoipAsnOrgScalar {}
impl VScalar for GeoipAsnOrgScalar {
    type State = GeoipASNState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        invoke_wrapper(
            state.reader.clone(),
            input,
            output,
            GeoipAsnOrgScalar::lookup_ip,
        )
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

impl GeoipAsnOrgScalar {
    fn lookup_ip(db: &maxminddb::Reader<Mmap>, ip: &ffi::duckdb_string_t) -> Option<String> {
        //let as_str = ip_as_ref.data.as_ref()?;
        let as_ipaddr = duckdb_string_to_string(ip).parse().ok()?;
        if let Ok(asn_record) = db.lookup::<geoip2::Asn>(as_ipaddr) {
            return Some(
                asn_record?
                    .autonomous_system_organization
                    .unwrap_or("")
                    .to_string(),
            );
        };
        return None;
    }
}

pub struct GeoipAsnNumScalar {}
impl VScalar for GeoipAsnNumScalar {
    type State = GeoipASNState;

    unsafe fn invoke(
        state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        invoke_wrapper(
            state.reader.clone(),
            input,
            output,
            GeoipAsnNumScalar::lookup_ip,
        )
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

impl GeoipAsnNumScalar {
    fn lookup_ip(db: &maxminddb::Reader<Mmap>, ip: &ffi::duckdb_string_t) -> Option<String> {
        //let as_str = ip_as_ref.data.as_ref()?;
        let as_ipaddr = duckdb_string_to_string(ip).parse().ok()?;
        if let Ok(asn_record) = db.lookup::<geoip2::Asn>(as_ipaddr) {
            return Some(
                asn_record?
                    .autonomous_system_number
                    .unwrap_or(0)
                    .to_string(),
            );
        };
        return None;
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    let _ = con.register_scalar_function::<GeoipAsnOrgScalar>("geoip_asn_org");
    let _ = con.register_scalar_function::<GeoipAsnNumScalar>("geoip_asn_num");
    Ok(())
}
