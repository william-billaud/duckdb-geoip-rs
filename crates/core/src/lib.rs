use duckdb_neo::{
    DuckDBType,
    error::DuckDBError,
    extension::Extension,
    scalar::{ScalarCallbacks, ScalarFunctionBuilder, ScalarInitCallbacks},
    signature::{FunctionDocumentationBuilder, FunctionParameter, SignatureBuilder},
};

use duckdb_neo::vector::WritableVectorElement;
use maxminddb;
use std::env;
use std::marker::PhantomData;
use std::net::IpAddr;
use std::path::Path;

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
    fn get_db(&self) -> Result<maxminddb::Reader<Vec<u8>>, duckdb_neo::error::Error> {
        let dbpath_s =
            env::var("MAXMIND_MMDB_DIR").unwrap_or_else(|_| "/usr/share/GeoIP".to_string());
        let dbpath = Path::new(&dbpath_s).join(self.db_name());
        match maxminddb::Reader::open_readfile(&dbpath) {
            Ok(db) => Ok(db),
            Err(err) => Err(duckdb_neo::Error {
                code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                message: format!(
                    "initialize mmdb file failed. {} : {}",
                    dbpath.display(),
                    err
                )
                .to_string(),
            }),
        }
    }
}

struct CountryIsoLookup;
struct CityLookup;

struct ASNOrgLookup;
struct ASNLookup;

trait GeoipOutput: WritableVectorElement {
    fn write_value(
        writer: &mut duckdb_neo::vector::VectorWriter<'_, '_, Self>,
        index: usize,
        value: Option<Self>,
    ) -> duckdb_neo::Result<()>;
}

impl GeoipOutput for String {
    fn write_value(
        writer: &mut duckdb_neo::vector::VectorWriter<'_, '_, Self>,
        index: usize,
        value: Option<Self>,
    ) -> duckdb_neo::Result<()> {
        match value {
            Some(value) => writer.set(index, Some(value.as_str())),
            None => writer.set(index, None),
        }
    }
}
impl GeoipOutput for u32 {
    fn write_value(
        writer: &mut duckdb_neo::vector::VectorWriter<'_, '_, Self>,
        index: usize,
        value: Option<Self>,
    ) -> duckdb_neo::Result<()> {
        writer.set(index, value)
    }
}

trait GeoipLookup {
    type Output: GeoipOutput;
    fn lookup_ip(db: &mut maxminddb::Reader<Vec<u8>>, ip_as_str: &str) -> Option<Self::Output>;
    fn database_type() -> MMDBDatabaseType;
}

impl GeoipLookup for CountryIsoLookup {
    type Output = String;
    fn lookup_ip(db: &mut maxminddb::Reader<Vec<u8>>, ip_as_str: &str) -> Option<String> {
        match ip_as_str.parse::<IpAddr>() {
            Ok(ip) => db.lookup(ip).ok().and_then(move |result| {
                result
                    .decode_path(&maxminddb::path!["country", "iso_code"])
                    .unwrap_or(Some(""))
                    .or(Some(""))
                    .and_then(|name| Some(name.to_string()))
            }),
            Err(_) => None,
        }
    }

    fn database_type() -> MMDBDatabaseType {
        MMDBDatabaseType::City
    }
}
impl GeoipLookup for CityLookup {
    type Output = String;
    fn lookup_ip(db: &mut maxminddb::Reader<Vec<u8>>, ip_as_str: &str) -> Option<String> {
        match ip_as_str.parse::<IpAddr>() {
            Ok(ip) => db.lookup(ip).ok().and_then(move |result| {
                result
                    .decode_path(&maxminddb::path!["city", "names", "en"])
                    .unwrap_or(Some(""))
                    .or(Some(""))
                    .and_then(|name| Some(name.to_string()))
            }),
            Err(_) => None,
        }
    }

    fn database_type() -> MMDBDatabaseType {
        MMDBDatabaseType::City
    }
}

impl GeoipLookup for ASNOrgLookup {
    type Output = String;
    fn lookup_ip(db: &mut maxminddb::Reader<Vec<u8>>, ip_as_str: &str) -> Option<String> {
        match ip_as_str.parse::<IpAddr>() {
            Ok(ip) => db.lookup(ip).ok().and_then(move |result| {
                result
                    .decode_path(&maxminddb::path!["autonomous_system_organization"])
                    .unwrap_or(Some(""))
                    .or(Some(""))
                    .and_then(|name| Some(name.to_string()))
            }),
            Err(_) => None,
        }
    }

    fn database_type() -> MMDBDatabaseType {
        MMDBDatabaseType::Asn
    }
}

impl GeoipLookup for ASNLookup {
    type Output = u32;
    fn lookup_ip(db: &mut maxminddb::Reader<Vec<u8>>, ip_as_str: &str) -> Option<u32> {
        match ip_as_str.parse::<IpAddr>() {
            Ok(ip) => db.lookup(ip).ok().and_then(|result| {
                result
                    .decode_path(&maxminddb::path!["autonomous_system_number"])
                    .unwrap_or(None)
            }),
            Err(_) => None,
        }
    }

    fn database_type() -> MMDBDatabaseType {
        MMDBDatabaseType::Asn
    }
}

struct GeoipScalar<L: GeoipLookup> {
    _lookup: PhantomData<fn() -> L>,
}

impl<L: GeoipLookup + 'static> GeoipScalar<L> {
    pub fn new() -> Self {
        Self {
            _lookup: PhantomData,
        }
    }
}
impl<L: GeoipLookup + 'static> ScalarInitCallbacks for GeoipScalar<L> {
    fn init(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: &duckdb_neo::connection::Context,
    ) -> duckdb_neo::Result<Self::InitData> {
        L::database_type().get_db()
    }
}
impl<L: GeoipLookup + 'static> ScalarCallbacks for GeoipScalar<L> {
    type BindData = ();
    type InitData = maxminddb::Reader<Vec<u8>>;
    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        init_data: Option<&mut Self::InitData>,
        _context: &duckdb_neo::connection::Context,
        vectors: &mut duckdb_neo::data_chunk::VectorCollection,
        output: duckdb_neo::vector::Vector<'_, duckdb_neo::vector::Unknown>,
    ) -> duckdb_neo::Result<()> {
        let mut output = output.cast::<L::Output>()?;
        output.set_size(vectors.row_count()?)?;

        let vector = vectors.get_vector_at::<String>(0)?;
        let reader = vector.reader()?;
        let mut writer = output.writer()?;
        let mmdb_reader = match init_data {
            Some(mmdb_reader) => mmdb_reader,
            None => {
                return Err(duckdb_neo::Error {
                    code: DuckDBError::DUCKDB_V2_ERROR_INPUT_INVALID,
                    message: "initialize mmdb file failed".to_string(),
                });
            }
        };

        for (i, row) in reader.iter().enumerate() {
            match row {
                Some(row) => L::Output::write_value(&mut writer, i, L::lookup_ip(mmdb_reader, row)),
                None => writer.set(i, None),
            }?
        }
        Ok(())
    }
}

pub fn register(extension: &Extension) -> duckdb_neo::Result<()> {
    ScalarFunctionBuilder::new(
        "geoip_country_iso",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("input"))
            .return_type(String::logical_type(extension)?)
            .documentation(
                FunctionDocumentationBuilder::new()
                    .add_category("geoip")
                    .add_example("geoip_country_iso", "geoip_country_iso('90.9.250.1')")
                    .description("Geoip country lookup using `$MAXMIND_MMDB_DIR`/GeoLite2-City.mmdb database"),
            ),
        GeoipScalar::<CountryIsoLookup>::new(),
    )
    .with_init()
    .register(extension)?;

    ScalarFunctionBuilder::new(
        "geoip_city",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("input"))
            .return_type(String::logical_type(extension)?)
            .documentation(
                FunctionDocumentationBuilder::new()
                    .add_category("geoip")
                    .add_example("geoip_city", "geoip_city('90.9.250.1')")
                    .description(
                        "Geoip city lookup using `$MAXMIND_MMDB_DIR`/GeoLite2-City.mmdb database",
                    ),
            ),
        GeoipScalar::<CityLookup>::new(),
    )
    .with_init()
    .register(extension)?;

    ScalarFunctionBuilder::new(
        "geoip_asn_org",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("input"))
            .return_type(String::logical_type(extension)?)
            .documentation(
                FunctionDocumentationBuilder::new()
                    .add_category("geoip")
                    .add_example("geoip_asn_org", "geoip_asn_org('90.9.250.1')")
                    .description(
                        "IP ASN org lookup using `$MAXMIND_MMDB_DIR`/GeoLite2-ASN.mmdb database",
                    ),
            ),
        GeoipScalar::<ASNOrgLookup>::new(),
    )
    .with_init()
    .register(extension)?;

    ScalarFunctionBuilder::new(
        "geoip_asn_num",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("input"))
            .return_type(u32::logical_type(extension)?)
            .documentation(
                FunctionDocumentationBuilder::new()
                    .add_category("geoip")
                    .add_example("geoip_asn_num", "geoip_asn_num('90.9.250.1')")
                    .description(
                        "IP ASN lookup using `$MAXMIND_MMDB_DIR`/GeoLite2-ASN.mmdb database",
                    ),
            ),
        GeoipScalar::<ASNLookup>::new(),
    )
    .with_init()
    .register(extension)?;

    Ok(())
}
