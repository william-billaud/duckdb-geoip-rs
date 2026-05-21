use duckdb::{
    duckdb_entrypoint_c_api,
    vscalar::{ArrowFunctionSignature, VArrowScalar},
    Connection, Result,
};

use arrow::{
    array::{Array, StringArray},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use maxminddb::geoip2;
use maxminddb::Mmap;
use once_cell::sync::OnceCell;
use std::env;
use std::error::Error;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;

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
        let reader = unsafe { maxminddb::Reader::open_mmap(dbpath.join(self.db_name())) }.expect(
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
    input: RecordBatch,
    geoip_func: fn(db: &maxminddb::Reader<Mmap>, ip: IpAddr) -> Option<String>,
) -> Result<Arc<dyn Array>, Box<dyn Error>> {
    let reader = match mmdb_type {
        MMDBDatabaseType::City => MMDB_CITY_CELL.get_or_init(|| mmdb_type.get_db()),
        MMDBDatabaseType::Asn => MMDB_ASN_CELL.get_or_init(|| mmdb_type.get_db()),
    };
    let input_vector = input
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let results: Vec<Option<String>> = input_vector
        .iter()
        .map(|i| -> Result<Option<String>, Box<dyn Error>> {
            Ok(i.and_then(|input_str| {
                input_str
                    .parse()
                    .ok()
                    .and_then(|as_ip| Some(geoip_func(reader, as_ip).unwrap_or("".to_string())))
            }))
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(StringArray::from(results)))
}

pub struct GeoipAsnOrgScalar {}
impl VArrowScalar for GeoipAsnOrgScalar {
    type State = ();

    fn invoke(_: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        invoke_wrapper(MMDBDatabaseType::Asn, input, GeoipAsnOrgScalar::lookup_ip)
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Utf8,
        )]
    }

    fn volatile() -> bool {
        false
    }
}

impl GeoipAsnOrgScalar {
    fn lookup_ip(db: &maxminddb::Reader<Mmap>, ip: IpAddr) -> Option<String> {
        //let as_str = ip_as_ref.data.as_ref()?;
        db.lookup(ip)
            .ok()
            .and_then(|result| result.decode::<geoip2::Asn>().ok())?
            .and_then(|asn_record| asn_record.autonomous_system_organization)
            .and_then(|organization| Some(organization.to_string()))
    }
}

pub struct GeoipAsnNumScalar {}
impl VArrowScalar for GeoipAsnNumScalar {
    type State = ();

    fn invoke(_: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        invoke_wrapper(MMDBDatabaseType::Asn, input, GeoipAsnNumScalar::lookup_ip)
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Utf8,
        )]
    }

    fn volatile() -> bool {
        false
    }
}

impl GeoipAsnNumScalar {
    fn lookup_ip(db: &maxminddb::Reader<Mmap>, ip: IpAddr) -> Option<String> {
        db.lookup(ip)
            .ok()
            .and_then(|result| result.decode::<geoip2::Asn>().ok())?
            .and_then(|asn_record| asn_record.autonomous_system_number)
            .and_then(|number| Some(number.to_string()))
    }
}

pub struct GeoipCityScalar {}
impl VArrowScalar for GeoipCityScalar {
    type State = ();

    fn invoke(_: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        invoke_wrapper(MMDBDatabaseType::City, input, GeoipCityScalar::lookup_ip)
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Utf8,
        )]
    }

    fn volatile() -> bool {
        false
    }
}

impl GeoipCityScalar {
    fn lookup_ip(db: &maxminddb::Reader<Mmap>, ip: IpAddr) -> Option<String> {
        match db
            .lookup(ip)
            .ok()
            .and_then(|result| result.decode::<geoip2::City>().ok())?
        {
            // only support english, maybe allows to pass language as param
            Some(city) => Some(city.city.names.english?.to_string()),
            None => None,
        }
    }
}

pub struct GeoipCountryIsoScalar {}
impl VArrowScalar for GeoipCountryIsoScalar {
    type State = ();

    fn invoke(_: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        invoke_wrapper(
            MMDBDatabaseType::City,
            input,
            GeoipCountryIsoScalar::lookup_ip,
        )
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8],
            DataType::Utf8,
        )]
    }

    fn volatile() -> bool {
        false
    }
}

impl GeoipCountryIsoScalar {
    fn lookup_ip(db: &maxminddb::Reader<Mmap>, ip: IpAddr) -> Option<String> {
        match db
            .lookup(ip)
            .ok()
            .and_then(|result| result.decode::<geoip2::City>().ok())?
        {
            // only support english, maybe allows to pass language as param
            Some(city) => Some(city.country.iso_code?.to_string()),
            None => None,
        }
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
