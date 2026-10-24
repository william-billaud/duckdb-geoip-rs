use duckdb_neo::{
    DuckDBType, Result,
    extension::Extension,
    scalar::{ScalarCallbacks, ScalarFunctionBuilder},
    signature::{FunctionDocumentationBuilder, FunctionParameter, SignatureBuilder},
};

use maxminddb::Mmap;
use maxminddb::geoip2;
use once_cell::sync::OnceCell;
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

pub struct GeoipCityScalar {}

impl ScalarCallbacks for GeoipCityScalar {
    type BindData = ();
    type InitData = ();
    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &duckdb_neo::connection::Context,
        vectors: &mut duckdb_neo::data_chunk::VectorCollection,
        output: duckdb_neo::vector::Vector<'_, duckdb_neo::vector::Unknown>,
    ) -> Result<()> {
        let mut output = output.cast::<String>()?;
        output.set_size(vectors.row_count()?)?;

        let vector = vectors.get_vector_at::<String>(0)?;
        let reader = vector.reader()?;
        let mut writer = output.writer()?;
        let mmdb_reader = MMDB_CITY_CELL.get_or_init(|| MMDBDatabaseType::City.get_db());

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
    fn lookup_ip(db: &maxminddb::Reader<Mmap>, ip: IpAddr) -> Option<&str> {
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

impl ScalarCallbacks for GeoipAsnOrgScalar {
    type BindData = ();
    type InitData = ();
    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &duckdb_neo::connection::Context,
        vectors: &mut duckdb_neo::data_chunk::VectorCollection,
        output: duckdb_neo::vector::Vector<'_, duckdb_neo::vector::Unknown>,
    ) -> Result<()> {
        let mut output = output.cast::<String>()?;
        output.set_size(vectors.row_count()?)?;

        let vector = vectors.get_vector_at::<String>(0)?;
        let reader = vector.reader()?;
        let mut writer = output.writer()?;
        let mmdb_reader = MMDB_ASN_CELL.get_or_init(|| MMDBDatabaseType::Asn.get_db());

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
    fn lookup_ip(db: &maxminddb::Reader<Mmap>, ip: IpAddr) -> Option<&str> {
        db.lookup(ip)
            .ok()
            .and_then(|result| result.decode::<geoip2::Asn>().ok())?
            .and_then(|asn_record| asn_record.autonomous_system_organization)
            .and_then(|organization| Some(organization))
    }
}

struct GeoipAsnNumScalar {}

impl ScalarCallbacks for GeoipAsnNumScalar {
    type BindData = ();
    type InitData = ();
    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &duckdb_neo::connection::Context,
        vectors: &mut duckdb_neo::data_chunk::VectorCollection,
        output: duckdb_neo::vector::Vector<'_, duckdb_neo::vector::Unknown>,
    ) -> Result<()> {
        let mut output = output.cast::<u32>()?;
        output.set_size(vectors.row_count()?)?;

        let vector = vectors.get_vector_at::<String>(0)?;
        let reader = vector.reader()?;
        let mut writer = output.writer()?;
        let mmdb_reader = MMDB_ASN_CELL.get_or_init(|| MMDBDatabaseType::Asn.get_db());

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
    fn lookup_ip(db: &maxminddb::Reader<Mmap>, ip: IpAddr) -> Option<u32> {
        db.lookup(ip)
            .ok()
            .and_then(|result| result.decode::<geoip2::Asn>().ok())?
            .and_then(|asn_record| asn_record.autonomous_system_number)
            .and_then(|number| Some(number))
    }
}

pub struct GeoipCountryIsoScalar {}

impl ScalarCallbacks for GeoipCountryIsoScalar {
    type BindData = ();
    type InitData = ();
    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &duckdb_neo::connection::Context,
        vectors: &mut duckdb_neo::data_chunk::VectorCollection,
        output: duckdb_neo::vector::Vector<'_, duckdb_neo::vector::Unknown>,
    ) -> Result<()> {
        let mut output = output.cast::<String>()?;
        output.set_size(vectors.row_count()?)?;

        let vector = vectors.get_vector_at::<String>(0)?;
        let reader = vector.reader()?;
        let mut writer = output.writer()?;
        let mmdb_reader = MMDB_CITY_CELL.get_or_init(|| MMDBDatabaseType::City.get_db());

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
    fn lookup_ip(db: &maxminddb::Reader<Mmap>, ip: IpAddr) -> Option<&str> {
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

pub fn register(extension: &Extension) -> Result<()> {
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
    .register(extension)?;

    Ok(())
}
