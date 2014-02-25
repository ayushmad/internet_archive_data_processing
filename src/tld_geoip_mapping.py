import os
import sys
import global_file
import dns.resolver
import geoip
import traceback
import sys

class TldGeoipMapping:
    def __init__ (self, source_file, dest_file, tld_set):
        self.source_fn = source_file;
        self.dest_fn = dest_file;
        self.tld_set = tld_set;
        return;

    def get_domain (self, url):
        url_elements = url.split('.');
        for i in range(-len(url_elements), 0):
            last_i_elements = url_elements[i:];
            candidate = ".".join(last_i_elements);
            wildcard_candidate = ".".join(["*"] + last_i_elements[1:]);
            exception_candidate = "!" + candidate;

            # match tlds: 
            if (exception_candidate in self.tld_set):
                return ".".join(url_elements[i:]) ;
            if (candidate in self.tld_set or wildcard_candidate in self.tld_set):
                return ".".join(url_elements[i-1:]);
        return None;

    def get_url_from_line (self, line):
        return line.split(global_file.DELIMITER)[0];

    def get_ip (self, tld):
        try:
            res = self.ds.query(tld)[0].to_text();
        except Exception as e:
            res = '0.0.0.0';
        return res;

    def geoip_mapping (self, ip):
        if ip == '0.0.0.0':
            return "UNKNOWN";
        try:
            country = geoip.country(ip,
                                    global_file.GEOIP_MAP);
        except Exception as e:
            country = "UNKNOWN";
        return country;

    def open_files (self):
        self.source_fh = open(self.source_fn, "r");
        self.dest_fh = open(self.dest_fn, "w");
        self.ds = dns.resolver;
        return;
    
    def close_files (self):
        self.source_fh.close();
        self.dest_fh.close();
        return;

    def process (self):
        self.open_files();
        for line in self.source_fh:
            url = self.get_url_from_line(line);
            tld = self.get_domain(url);
            ip = self.get_ip(tld);
            country = self.geoip_mapping(ip);
            self.dest_fh.write("%s%s%s%s%s%s%s\n"%(url,
                                                 global_file.DELIMITER,
                                                 tld,
                                                 global_file.DELIMITER,
                                                 ip,
                                                 global_file.DELIMITER,
                                                 country));
        self.close_files();


if __name__ == "__main__":
    tld_file = open(global_file.TLD_FILE);
    tld = set([line.strip() for line in tld_file if line[0] not in "/\n"]);

    tld = TldGeoipMapping(sys.argv[1],
                          sys.argv[2],
                          tld);
    tld.process();
