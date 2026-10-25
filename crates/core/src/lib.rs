use duckdb_neo::{
    DuckDBType,
    error::DuckDBError,
    extension::Extension,
    scalar::{ScalarCallbacks, ScalarFunctionBuilder, ScalarInitCallbacks},
    signature::{FunctionDocumentationBuilder, FunctionParameter, SignatureBuilder},
};

use maxminddb;
use maxminddb::geoip2;
use std::env;
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
pub struct GeoipCityScalar {}

impl ScalarInitCallbacks for GeoipCityScalar {
    fn init(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: &duckdb_neo::connection::Context,
    ) -> duckdb_neo::Result<Self::InitData> {
        MMDBDatabaseType::City.get_db()
    }
}
impl ScalarCallbacks for GeoipCityScalar {
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
        let mut output = output.cast::<String>()?;
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
                Some(row) => writer.set(
                    i,
                    match { row.parse::<IpAddr>() } {
                        Ok(ip) => Self::lookup_ip(mmdb_reader, ip).or(Some("")),
                        Err(_) => None,
                    },
                ),
                None => writer.set(i, None),
            }?
        }
        Ok(())
    }
}

impl GeoipCityScalar {
    fn lookup_ip(db: &maxminddb::Reader<Vec<u8>>, ip: IpAddr) -> Option<&str> {
        match db
            .lookup(ip)
            .ok()
            .and_then(|result| result.decode::<geoip2::City>().ok())?
        {
            // only support english, maybe allows to pass language as param
            Some(city) => Some(city.city.names.english?),
            None => None,
        }
    }
}
pub struct GeoipAsnOrgScalar {}

impl ScalarInitCallbacks for GeoipAsnOrgScalar {
    fn init(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: &duckdb_neo::connection::Context,
    ) -> duckdb_neo::Result<Self::InitData> {
        MMDBDatabaseType::Asn.get_db()
    }
}

impl ScalarCallbacks for GeoipAsnOrgScalar {
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
        let mut output = output.cast::<String>()?;
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
                Some(row) => writer.set(
                    i,
                    match { row.parse::<IpAddr>() } {
                        Ok(ip) => Self::lookup_ip(mmdb_reader, ip),
                        Err(_) => None,
                    },
                ),
                None => writer.set(i, None),
            }?
        }
        Ok(())
    }
}

impl GeoipAsnOrgScalar {
    fn lookup_ip(db: &maxminddb::Reader<Vec<u8>>, ip: IpAddr) -> Option<&str> {
        db.lookup(ip)
            .ok()
            .and_then(|result| result.decode::<geoip2::Asn>().ok())?
            .and_then(|asn_record| asn_record.autonomous_system_organization)
            .and_then(|organization| Some(organization))
    }
}

struct GeoipAsnNumScalar {}

impl ScalarInitCallbacks for GeoipAsnNumScalar {
    fn init(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: &duckdb_neo::connection::Context,
    ) -> duckdb_neo::Result<Self::InitData> {
        MMDBDatabaseType::Asn.get_db()
    }
}

impl ScalarCallbacks for GeoipAsnNumScalar {
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
        let mut output = output.cast::<u32>()?;
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
                Some(row) => writer.set(
                    i,
                    match { row.parse::<IpAddr>() } {
                        Ok(ip) => Self::lookup_ip(mmdb_reader, ip),
                        Err(_) => None,
                    },
                ),
                None => writer.set(i, None),
            }?
        }
        Ok(())
    }
}

impl GeoipAsnNumScalar {
    fn lookup_ip(db: &maxminddb::Reader<Vec<u8>>, ip: IpAddr) -> Option<u32> {
        db.lookup(ip)
            .ok()
            .and_then(|result| result.decode::<geoip2::Asn>().ok())?
            .and_then(|asn_record| asn_record.autonomous_system_number)
            .and_then(|number| Some(number))
    }
}

pub struct GeoipCountryIsoScalar {}

impl ScalarInitCallbacks for GeoipCountryIsoScalar {
    fn init(
        &self,
        _bind_data: Option<&Self::BindData>,
        _context: &duckdb_neo::connection::Context,
    ) -> duckdb_neo::Result<Self::InitData> {
        MMDBDatabaseType::City.get_db()
    }
}

impl ScalarCallbacks for GeoipCountryIsoScalar {
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
        let mut output = output.cast::<String>()?;
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
                Some(row) => writer.set(
                    i,
                    match { row.parse::<IpAddr>() } {
                        Ok(ip) => Self::lookup_ip(mmdb_reader, ip),
                        Err(_) => None,
                    },
                ),
                None => writer.set(i, None),
            }?
        }
        Ok(())
    }
}

impl GeoipCountryIsoScalar {
    fn lookup_ip(db: &maxminddb::Reader<Vec<u8>>, ip: IpAddr) -> Option<&str> {
        match db
            .lookup(ip)
            .ok()
            .and_then(|result| result.decode::<geoip2::City>().ok())?
        {
            // only support english, maybe allows to pass language as param
            Some(city) => Some(city.country.iso_code?),
            None => None,
        }
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
        GeoipCountryIsoScalar {},
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
        GeoipCityScalar {},
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
        GeoipAsnOrgScalar {},
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
        GeoipAsnNumScalar {},
    )
    .with_init()
    .register(extension)?;

    Ok(())
}
