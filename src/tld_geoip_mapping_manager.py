import os
import process_manager
import global_file
import glob
import tld_geoip_mapping
import time

class TldGeoIpMappingManager:
    def __init__ (self, subdir):
        temp_dir = global_file.get_temp_dir();
        self.temp_dir = os.path.join(temp_dir,
                                     subdir,
                                     global_file.GEOIP_MAPPER_DIR);
        input_dir = os.path.join(global_file.get_interm_dir(),
                                 subdir,
                                 global_file.MERGE_NODES);
        self.input_file = glob.glob(input_dir + "/*")[0];
        self.output_dir = os.path.join(global_file.get_interm_dir(),
                                       subdir,
                                       global_file.GEOIP_MAPPER_DIR);
        global_file.create_if_not_exist(self.temp_dir);
        global_file.create_if_not_exist(self.output_dir);
        self.split_size = 2000;
        return;

    def open_files(self):
        self.input_fh = open(self.input_file, "r");
        return;
    
    def aggreate_results(self, out_file_list):
        geoip_out_fh = open(os.path.join(self.output_dir,
                                          "geoip.nodes"),
                            "w");
        for out_fn in out_file_list:
            out_fh = open(out_fn,
                          "r");
            for line in out_fh:
                geoip_out_fh.write(line);
            out_fh.close();
        geoip_out_fh.close();
    
    def load_tld(self):
        tld_file = open(global_file.TLD_FILE);
        self.tld = set([line.strip() for line in tld_file if line[0] not in "/\n"]);
        return;

    def process(self):
        self.open_files();
        self.load_tld();
        global_file.clean_dir(self.temp_dir);
        # writing temp_file
        group_counter = 0;
        out_file_list = [];
        pm = process_manager.ProcessManager(15);
        pm.start_workers();
        line = self.input_fh.readline();
        while line != '':
            counter = self.split_size;
            temp_in_file_block = os.path.join(self.temp_dir,
                                              str(group_counter)+".in");
            temp_out_file_block = os.path.join(self.temp_dir,
                                               str(group_counter) + ".out");
            temp_in_fh = open(temp_in_file_block, "w");
            while counter > 0 and line != '':
                temp_in_fh.write(line);
                line = self.input_fh.readline();
                counter -= 1;
            temp_in_fh.close();
            dm = tld_geoip_mapping.TldGeoipMapping(temp_in_file_block, 
                                                   temp_out_file_block,
                                                   self.tld);
            pm.add_jobs(dm);
            out_file_list.append(temp_out_file_block);
            group_counter += 1;
        # closing input_files
        while not pm.are_all_jobs_completed():
            time.sleep(5);
        pm.close();
        self.aggreate_results(out_file_list);
        return;


if __name__ == "__main__":
    tgmm = TldGeoIpMappingManager('109');
    tgmm.process();
