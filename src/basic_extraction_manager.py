import os
import sys
import  process_manager
import basic_extraction
import time
import glob
import global_file
    


class BasicExtractionManager:
    def __init__ (self, subdir):
        self.input_dir = os.path.join(global_file.get_input_dir(), 
                                      subdir);
        interm_dir = global_file.get_interm_dir();
        interm_dir = os.path.join(interm_dir,
                                  subdir); 
        self.node_dir = os.path.join(interm_dir,
                                     global_file.BASIC_NODES);
        self.edge_dir = os.path.join(interm_dir,
                                     global_file.BASIC_EDGES);
        global_file.create_if_not_exist(interm_dir);
        global_file.create_if_not_exist(self.node_dir);
        global_file.create_if_not_exist(self.edge_dir);
        return;
    
    def get_source_files(self):
        file_list = glob.glob(self.input_dir +"/*");
        return file_list;

    def process(self, wait=True):
        pm = process_manager.ProcessManager();
        file_list = self.get_source_files();
        pm.start_workers();
        for my_file in file_list:
            be = basic_extraction.BasicExtraction(my_file,
                                                  self.node_dir,
                                                  self.edge_dir);
            pm.add_jobs(be);
        if wait:
            while not pm.are_all_jobs_completed():
                time.sleep(5);
            pm.close();
        return;

if __name__ == "__main__":
    bem = BasicExtractionManager("109");
    bem.process();
