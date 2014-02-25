import global_file
import sys

class MergeFiles:
    def __init__ (self, sf1, sf2, of, file_type):
        self.source_fn1 = sf1;
        self.source_fn2 = sf2;
        self.out_fn = of;
        self.file_type = file_type;
        return;

    def open_source_output_files (self):
        self.source_fh1 = open(self.source_fn1, "r");
        self.source_fh2 = open(self.source_fn2, "r");
        self.out_fh = open(self.out_fn, "w");
        return;

    def compare_lines (self, line1, line2):
        if self.file_type == "node":
            dom1 = line1.split(global_file.DELIMITER)[0].strip();
            dom2 = line2.split(global_file.DELIMITER)[0].strip();
            if dom1 > dom2:
                return 1;
            elif dom1 < dom2:
                return -1;
            else:
                return 0;
        elif self.file_type == "edge":
            (src1, dest1) = line1.split(global_file.DELIMITER)[:2];
            (src2, dest2) = line2.split(global_file.DELIMITER)[:2];
            if src1 > src2:
                return 1;
            elif src1 < src2:
                return -1;
            else:
                if dest1 > dest2:
                    return 1;
                elif dest1 < dest2:
                    return -1;
                else:
                    return 0;
        # if it comes here raise error
        return;
    
    def add_lines(self, line1, line2):
        if self.file_type == "node":
            (dom1, indig1, outdig1) = line1.split(global_file.DELIMITER);
            (dom2, indig2, outdig2) = line2.split(global_file.DELIMITER);
            return "%s%s%d%s%d\n"%(dom1,
                                 global_file.DELIMITER,
                                 int(indig1) + int(indig2),
                                 global_file.DELIMITER,
                                 int(outdig1) + int(outdig2));
        else:
            (src1, dest1, edge_cnt1) = line1.split(global_file.DELIMITER);
            (src2, dest2, edge_cnt2) = line2.split(global_file.DELIMITER);
            return "%s%s%s%s%d\n"%(src1,
                                 global_file.DELIMITER,
                                 dest1,
                                 global_file.DELIMITER,
                                 int(edge_cnt1) + int(edge_cnt2));
        return;
    
    def close_files(self):
        self.source_fh1.close();
        self.source_fh2.close();
        self.out_fh.close();
        return;

    def process (self):
        self.open_source_output_files();
        line1 = self.source_fh1.readline();
        line2 = self.source_fh2.readline();
        while line1 != "" and line2 != "":
            comp_res = self.compare_lines(line1, line2);
            if comp_res == 1:
                self.out_fh.write(line2);
                line2 = self.source_fh2.readline();
            elif comp_res == -1:
                self.out_fh.write(line1);
                line1 = self.source_fh1.readline();
            else:
                self.out_fh.write(self.add_lines(line1, line2));
                line1 = self.source_fh1.readline();
                line2 = self.source_fh2.readline();
        while line1 != "":
            self.out_fh.write(line1);
            line1 = self.source_fh1.readline();

        while line2 != "":
            self.out_fh.write(line2);
            line2 = self.source_fh2.readline();
        self.close_files();
        return;

if __name__ == "__main__":
   mf = MergeFiles(sys.argv[1], 
                   sys.argv[2], 
                   "merge_test",
                   sys.argv[3]);
   mf.process();
