import os
import global_file
import glob
import process_manager
import merge_files
import time


class MergeFilesManager:
    def __init__ (self, subdir):
        input_dir = os.path.join(global_file.get_interm_dir(),
                                 subdir);
        self.input_node_dir = os.path.join(input_dir,
                                           global_file.BASIC_NODES);
        self.input_edge_dir = os.path.join(input_dir,
                                           global_file.BASIC_EDGES);
        self.temp_nodes_dir = os.path.join(global_file.get_temp_dir(),
                                           subdir,
                                           global_file.BASIC_NODES);
        self.temp_edges_dir = os.path.join(global_file.get_temp_dir(),
                                           subdir,
                                           global_file.BASIC_EDGES);
        self.out_nodes_dir = os.path.join(input_dir,
                                          global_file.MERGE_NODES);
        self.out_edges_dir = os.path.join(input_dir,
                                          global_file.MERGE_EDGES);
        global_file.create_if_not_exist(self.temp_nodes_dir);
        global_file.create_if_not_exist(self.temp_edges_dir);
        global_file.create_if_not_exist(self.out_nodes_dir);
        global_file.create_if_not_exist(self.out_edges_dir);
        return;

    def clean_temp_dirs (self):
        global_file.clean_dir(self.temp_nodes_dir);
        global_file.clean_dir(self.temp_edges_dir);
        return;
    
    def move_files_into_temp (self):
        # moving edge files
        global_file.copy_file(self.input_node_dir + "/*",
                              self.temp_nodes_dir + "/.");
        global_file.copy_file(self.input_edge_dir + "/*",
                              self.temp_edges_dir + "/.");
        return;
    
    def get_files_based_on_level (self, file_dir, level):
        temp_list = [];
        if level == 0:
            temp_list = glob.glob(file_dir + "/*");
        else:
            temp_list = glob.glob(file_dir + "/*.level" + str(level));
        return temp_list;
    
    def bump_file_level(self, file_path, level):
        if level == 0:
            global_file.move_file(file_path,
                                  file_path + 
                                  ".level" + 
                                  str(level + 1));
        else:
            global_file.move_file(file_path,
                                  file_path.replace(".level" + str(level),
                                                    ".level" + str(level + 1)));
        return;
    
    def merge_files(self, merge_dir, file_type):
        level = 0;
        merge_file_list =  self.get_files_based_on_level(merge_dir, level);
        count = len(merge_file_list);
        pm = process_manager.ProcessManager();
        pm.start_workers();
        while count > 1:
            while count > 1:
                fh1 = merge_file_list[count - 1];
                fh2 = merge_file_list[count - 2];
                count = count - 2;
                if level == 0:
                    of = fh1 + ".level" + str(level + 1);
                else:
                    of = fh1.replace(".level" + str(level),
                                     ".level" + str(level + 1));
                mf = merge_files.MergeFiles(fh1,
                                            fh2,
                                            of,
                                            file_type);
                
                pm.add_jobs(mf);
            if count == 1:
                # one single file left bump it up
                self.bump_file_level(merge_file_list[count-1],
                                     level);
            # wait till processing is completed
            while not pm.are_all_jobs_completed():
                time.sleep(5);
            level = level + 1;
            merge_file_list = self.get_files_based_on_level(merge_dir, level);
            count = len(merge_file_list);
        pm.close();
        return merge_file_list[0];
    
    def move_files_to_output(self, merged_file, out_dir):
        global_file.move_file(merged_file, out_dir);
        return;

    def process(self):
        self.clean_temp_dirs();
        self.move_files_into_temp();
        # Processing node files first
        result_file = self.merge_files(self.temp_nodes_dir, "node");
        self.move_files_to_output(result_file, self.out_nodes_dir);
        result_file = self.merge_files(self.temp_edges_dir, "edge");
        self.move_files_to_output(result_file, self.out_edges_dir);
        return;

if __name__ == "__main__":
   mfm = MergeFilesManager('109'); 
   mfm.process();
