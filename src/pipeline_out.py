import global_file as gf
import os
import tld_geoip_mapping_manager
import domain_mapper_manager
import merge_files_manager
import basic_extraction_manager

def merge_and_write(year):
    gf.create_if_not_exist(gf.OUTPUT_DIR);
    
    out_node_fn = gf.OUTPUT_DIR + gf.OUT_TEMPLETE.replace(gf.OUT_YEAR_TEMPELETE, year) + ".nodes"; 
    out_node_fh = open(out_node_fn, 'w');
    out_edge_fn = gf.OUTPUT_DIR + gf.OUT_TEMPLETE.replace(gf.OUT_YEAR_TEMPELETE, year) + ".edges"; 

    
    geoip_dir = os.path.join(gf.get_interm_dir(),
                             year,
                             gf.GEOIP_MAPPER_DIR);
    domain_dir = os.path.join(gf.get_interm_dir(),
                             year,
                             gf.DOMAIN_MAPPER_DIR);
    node_dir = os.path.join(gf.get_interm_dir(),
                             year,
                             gf.MERGE_NODES);
    edge_dir = os.path.join(gf.get_interm_dir(),
                             year,
                             gf.MERGE_EDGES);
    year_files = {};
    year_files["geoip_file"] = [ os.path.join(geoip_dir, f) for f in os.listdir(geoip_dir) if os.path.isfile(os.path.join(geoip_dir, f)) ];
    year_files["domain_file"] = [ os.path.join(domain_dir, f) for f in os.listdir(domain_dir) if os.path.isfile(os.path.join(domain_dir, f)) ];
    year_files["node_file"] = [ os.path.join(node_dir, f) for f in os.listdir(node_dir) if os.path.isfile(os.path.join(node_dir, f)) ];
    year_files["edge_file"] = [ os.path.join(edge_dir, f) for f in os.listdir(edge_dir) if os.path.isfile(os.path.join(edge_dir, f)) ];
    
    
    # Now we write them 
    i_nfh = open(year_files["node_file"][0], "r");
    i_dfh = open(year_files["domain_file"][0], "r");
    i_gfh = open(year_files["geoip_file"][0], "r");
    for line in i_nfh:
        (url, indegree, out_degree) = line.strip().split(gf.DELIMITER);
        (url_d, cc_tld) = i_dfh.readline().strip().split(gf.DELIMITER);
        (url_g, crap, ip, ip_cc) = i_gfh.readline().strip().split(gf.DELIMITER);
        if url != url_d or url != url_g:
            print line;
            print url_d;
            print url_g;
            raise;
        out_node_fh.write("%s%s%s%s%s%s%s%s%s%s%s\n"%(url,
                                          gf.DELIMITER,
                                          indegree,
                                          gf.DELIMITER,
                                          out_degree,
                                          gf.DELIMITER,
                                          cc_tld,
                                          gf.DELIMITER,
                                          ip,
                                          gf.DELIMITER,
                                          ip_cc));
    
    out_node_fh.close();
    gf.move_file(year_files["edge_file"][0], out_edge_fn);

# complete_file line
years = ['109', '110', '111', '112'];
#years = ['111', '112'];
for year in years:
    bem = basic_extraction_manager.BasicExtractionManager(year);
    bem.process();
    print "Completed basic processing";
    mfm = merge_files_manager.MergeFilesManager(year); 
    mfm.process();
    print "Completed Merging"
    dm = domain_mapper_manager.DomainMapperManager(year);
    dm.process();
    print "Completed Domain manager";
    tgmm = tld_geoip_mapping_manager.TldGeoIpMappingManager(year);
    tgmm.process();
    merge_and_write(year);
