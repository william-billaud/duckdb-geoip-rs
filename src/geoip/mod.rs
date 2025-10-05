// Inspired from https://github.com/erichutchins/geoipsed/blob/main/src/geoip.rs
use maxminddb::geoip2;
use maxminddb::Mmap;
use std::env;
use std::net::IpAddr;
use std::path::Path;

pub(crate) struct GeoIPCityDatabase {
    city_reader: maxminddb::Reader<Mmap>,
}

pub(crate) struct GeoIPASNDatabase {
    asn_reader: maxminddb::Reader<Mmap>,
}

impl Default for GeoIPCityDatabase {
    fn default() -> Self {
        Self {
            city_reader: maxminddb::Reader::open_mmap("/usr/share/GeoIP/GeoLite2-City.mmdb")
                .expect("Could not read GeoLite2-City.mmdb"),
        }
    }
}

impl Default for GeoIPASNDatabase {
    fn default() -> Self {
        Self {
            asn_reader: maxminddb::Reader::open_mmap("/usr/share/GeoIP/GeoLite2-ASN.mmdb")
                .expect("Could not read GeoLite2-ASN.mmdb"),
        }
    }
}

impl GeoIPASNDatabase {
    pub fn new() -> Self {
        let dbpath_s =
            env::var("MAXMIND_MMDB_DIR").unwrap_or_else(|_| "/usr/share/GeoIP".to_string());
        let dbpath = Path::new(&dbpath_s);
        Self {
            asn_reader: maxminddb::Reader::open_mmap(dbpath.join("GeoLite2-ASN.mmdb"))
                .expect("Could not read GeoLite2-ASN.mmdb"),
        }
    }

}

impl GeoIPCityDatabase {
    pub fn new() -> Self {
        let dbpath_s =
            env::var("MAXMIND_MMDB_DIR").unwrap_or_else(|_| "/usr/share/GeoIP".to_string());
        let dbpath = Path::new(&dbpath_s);
        Self {
            city_reader: maxminddb::Reader::open_mmap(dbpath.join("GeoLite2-City.mmdb"))
                .expect("Could not read GeoLite2-City.mmdb"),
        }
    }
}
