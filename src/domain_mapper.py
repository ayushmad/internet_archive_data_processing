import os
import global_file
import sys

class DomainMapper:
    def __init__ (self, source_file, dest_file, tld_table):
        self.source_fn = source_file;
        self.dest_fn = dest_file;
        self.tld_table = tld_table;
        return;

    def open_source_dest_files (self):
        self.source_fh = open(self.source_fn, "r");
        self.dest_fh = open(self.dest_fn, "w");
        return;
    
    def close_source_dest_files (self):
        self.source_fh.close();
        self.dest_fh.close();
        return;
    
    def tld_get (self, suffix):
        if self.tld_table.has_key(suffix):
            return self.tld_table[suffix];
        return "UNKNOWN";
    
    def get_domain (self, url):
        suffix = url.strip('.').split('.')[-1];
        return suffix;

    def get_url (self, line):
        return line.strip().split(global_file.DELIMITER)[0];
    
    def process (self):
        self.open_source_dest_files();
        for line in self.source_fh:
            url = self.get_url(line);
            tld = self.tld_get(self.get_domain(url));
            self.dest_fh.write("%s%s%s\n"%(url,
                                           global_file.DELIMITER,
                                           tld));
        self.close_source_dest_files();
        return;

if __name__ == "__main__":
    cc_fh = open(global_file.CCTLD_FILE);
    cctld = {};
    for line in cc_fh:
        (cc, country) = line.split('=')
        cctld[cc.strip().lower()] = country.strip();
    dm = DomainMapper(sys.argv[1],
                      sys.argv[2],
                      cctld);
    dm.process();


