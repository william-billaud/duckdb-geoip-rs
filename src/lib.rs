use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    Connection, Result,
};
use libduckdb_sys as ffi;
use maxminddb::geoip2;
use maxminddb::Mmap;
use once_cell::sync::OnceCell;
use std::env;
use std::error::Error;
use std::path::Path;
// from https://github.com/alamminsalo/duckdb-ml
fn duckdb_string_to_string(word: &ffi::duckdb_string_t) -> String {
    // Convert a string from ffi to a Rust string
    unsafe {
        let len = ffi::duckdb_string_t_length(*word);
        let c_ptr = ffi::duckdb_string_t_data(word as *const _ as *mut _);
        let bytes = std::slice::from_raw_parts(c_ptr as *const u8, len as usize);
        String::from_utf8_lossy(bytes).into_owned()
    }
}

enum MMDBDatabaseType {
    City,
    Asn,
}

impl MMDBDatabaseType {
    fn db_name(&self) -> &'static str {
        match self {
            MMDBDatabaseType::City => "GeoLite2-City.mmdb",
            MMDBDatabaseType::Asn => "GeoLite2-ASN.mmdb",
        }
    }
    fn get_db(&self) -> maxminddb::Reader<Mmap> {
        let dbpath_s =
            env::var("MAXMIND_MMDB_DIR").unwrap_or_else(|_| "/usr/share/GeoIP".to_string());
        let dbpath = Path::new(&dbpath_s);
        let reader = maxminddb::Reader::open_mmap(dbpath.join(self.db_name())).expect(
            format!(
                "Could not load mmdb file, trying to read {}. Use MAXMIND_MMDB_DIR",
                dbpath.join(self.db_name()).display()
            )
            .as_str(),
        );
        reader
    }
}

static MMDB_ASN_CELL: OnceCell<maxminddb::Reader<Mmap>> = OnceCell::new();
static MMDB_CITY_CELL: OnceCell<maxminddb::Reader<Mmap>> = OnceCell::new();

fn invoke_wrapper(
    mmdb_type: MMDBDatabaseType,
    input: &mut DataChunkHandle,
    output: &mut dyn WritableVector,
    geoip_func: fn(db: &maxminddb::Reader<Mmap>, ip: &ffi::duckdb_string_t) -> Option<String>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let reader = match mmdb_type {
        MMDBDatabaseType::City => MMDB_CITY_CELL.get_or_init(|| mmdb_type.get_db()),
        MMDBDatabaseType::Asn => MMDB_ASN_CELL.get_or_init(|| mmdb_type.get_db()),
    };
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
                    geoip_func(reader, s)
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
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        invoke_wrapper(
            MMDBDatabaseType::Asn,
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
        db.lookup::<geoip2::Asn>(as_ipaddr)
            .ok()
            .and_then(|asn_record| asn_record?.autonomous_system_organization)
            .and_then(|organization| Some(organization.to_string()))
    }
}

pub struct GeoipAsnNumScalar {}
impl VScalar for GeoipAsnNumScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        invoke_wrapper(
            MMDBDatabaseType::Asn,
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
        db.lookup::<geoip2::Asn>(as_ipaddr)
            .ok()
            .and_then(|asn_record| asn_record?.autonomous_system_number)
            .and_then(|number| Some(number.to_string()))
    }
}

pub struct GeoipCityScalar {}
impl VScalar for GeoipCityScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        invoke_wrapper(
            MMDBDatabaseType::City,
            input,
            output,
            GeoipCityScalar::lookup_ip,
        )
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

impl GeoipCityScalar {
    fn lookup_ip(db: &maxminddb::Reader<Mmap>, ip: &ffi::duckdb_string_t) -> Option<String> {
        //let as_str = ip_as_ref.data.as_ref()?;
        let as_ipaddr = duckdb_string_to_string(ip).parse().ok()?;
        match db
            .lookup::<geoip2::City>(as_ipaddr)
            .ok()
            .and_then(|city| city?.city)
            .and_then(|c| c.names)
        {
            // only support english, maybe allows to pass language as param
            Some(name) => name.get("en").and_then(|n| Some(n.to_string())),
            None => None,
        }
    }
}

pub struct GeoipCountryIsoScalar {}
impl VScalar for GeoipCountryIsoScalar {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        invoke_wrapper(
            MMDBDatabaseType::City,
            input,
            output,
            GeoipCountryIsoScalar::lookup_ip,
        )
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeId::Varchar.into()],
            LogicalTypeId::Varchar.into(),
        )]
    }
}

impl GeoipCountryIsoScalar {
    fn lookup_ip(db: &maxminddb::Reader<Mmap>, ip: &ffi::duckdb_string_t) -> Option<String> {
        //let as_str = ip_as_ref.data.as_ref()?;
        let as_ipaddr = duckdb_string_to_string(ip).parse().ok()?;
        db.lookup::<geoip2::City>(as_ipaddr)
            .ok()
            .and_then(|city| city?.country?.iso_code)
            .and_then(|iso_code| Some(iso_code.to_string()))
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    let _ = con.register_scalar_function::<GeoipAsnOrgScalar>("geoip_asn_org");
    let _ = con.register_scalar_function::<GeoipAsnNumScalar>("geoip_asn_num");
    let _ = con.register_scalar_function::<GeoipCityScalar>("geoip_city");
    let _ = con.register_scalar_function::<GeoipCountryIsoScalar>("geoip_country_iso");
    Ok(())
}
