import os
import process_manager
import global_file
import glob
import domain_mapper
import time


class DomainMapperManager:
    def __init__ (self, subdir):
        temp_dir = global_file.get_temp_dir();
        self.temp_dir = os.path.join(temp_dir,
                                     subdir,
                                     global_file.DOMAIN_MAPPER_DIR);
        input_dir = os.path.join(global_file.get_interm_dir(),
                                 subdir,
                                 global_file.MERGE_NODES);
        self.input_file = glob.glob(input_dir + "/*")[0];
        self.output_dir = os.path.join(global_file.get_interm_dir(),
                                       subdir,
                                       global_file.DOMAIN_MAPPER_DIR);
        global_file.create_if_not_exist(self.temp_dir);
        global_file.create_if_not_exist(self.output_dir);
        self.split_size = 5000;
        return;

    def open_files(self):
        self.input_fh = open(self.input_file, "r");
        return;
    
    def aggreate_results(self, out_file_list):
        domain_out_fh = open(os.path.join(self.output_dir,
                                          "domain.nodes"),
                            "w");
        for out_fn in out_file_list:
            out_fh = open(out_fn,
                          "r");
            for line in out_fh:
                domain_out_fh.write(line);
            out_fh.close();
        domain_out_fh.close();
    
    def load_cctld(self):
        cc_fh = open(global_file.CCTLD_FILE);
        self.cctld = {};
        for line in cc_fh:
            (cc, country) = line.split('=')
            self.cctld[cc.strip().lower()] = country.strip();
        return;

    def process(self):
        self.open_files();
        self.load_cctld();
        global_file.clean_dir(self.temp_dir);
        # writing temp_file
        group_counter = 0;
        out_file_list = [];
        pm = process_manager.ProcessManager();
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
            dm = domain_mapper.DomainMapper(temp_in_file_block, 
                                            temp_out_file_block,
                                            self.cctld);
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
    dm = DomainMapperManager('109');
    dm.process();
