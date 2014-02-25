import urlparse
import os
import sys
import global_file




class BasicExtraction:
    def __init__ (self, source_file, dest_node_dir, dest_edge_dir):
        self.source_fn = source_file;
        base_file_name =  os.path.basename(source_file);
        self.dest_node_fn = os.path.join(dest_node_dir, base_file_name + ".nodes");
        self.dest_edge_fn = os.path.join(dest_edge_dir, base_file_name + ".edges");
        self.nodes = {};
        self.edges = {};
        if not os.path.exists(source_file):
            # raise Exception here
            pass;
        return;

    def open_source_and_dest_files (self):
        self.source_fh = open(self.source_fn, "r");
        self.dest_node_fh = open(self.dest_node_fn, "w");
        self.dest_edge_fh = open(self.dest_edge_fn, "w");
        return;

    def add_node (self, node, deg_type):
        if not self.nodes.has_key(node):
            self.nodes[node] = [0, 0];
        if deg_type == "src":
            self.nodes[node][1] += 1;
        elif deg_type == "dest":
            self.nodes[node][0] += 1;
        return;
   
    def filter_url(self, url):
        if url[:4] == "www.":
            return url[4:];
        if url[:3] == "www":
            return url[3:];
        return url;

    def add_edge (self, src, dest):
        if not self.edges.has_key(src):
            self.edges[src] = {};
        if not self.edges[src].has_key(dest):
            self.edges[src][dest] = 0;
        self.edges[src][dest] += 1;
        return;

    def break_line(self, line):
        line_ele = line.split();
        if len(line_ele) == 1:
            return (line.strip(), "NULL");
        elif len(line_ele) == 2:
            return line_ele;
        else:
            edge_ele = [];
            for ele in line_ele:
                if "http" in ele or "www" in ele:
                    edge_ele.append(ele);
                    if len(edge_ele) == 2:
                        return edge_ele;
                if edge_ele == 1:
                    # Randomly selected part
                    return (edge_ele[0], "NULL");
                else:
                    return (line[:2]);
        return;

    def extract_nodes (self, line):
        (src, dest) = self.break_line(line);
        src_domain = urlparse.urlparse(src).netloc;
        dest_domain = urlparse.urlparse(dest).netloc;
        return (self.filter_url(src_domain.strip().lower()), 
                self.filter_url(dest_domain.strip().lower()));
    
    def close_files (self):
        self.dest_node_fh.close();
        self.dest_edge_fh.close();
        self.source_fh.close();
        return;

    def write_data (self):
        for node in sorted(self.nodes.keys()):
            self.dest_node_fh.write("%s%s%d%s%d\n"%(node,
                                                  global_file.DELIMITER,
                                                  int(self.nodes[node][0]),
                                                  global_file.DELIMITER,
                                                  int(self.nodes[node][1])));
        for src in sorted(self.edges.keys()):
            for dest in sorted(self.edges[src].keys()):
                self.dest_edge_fh.write("%s%s%s%s%d\n"%(src,
                                                      global_file.DELIMITER,
                                                      dest,
                                                      global_file.DELIMITER,
                                                      self.edges[src][dest]));
        return;

    def process(self):
        self.open_source_and_dest_files();
        for line in self.source_fh:
            if line.strip() == "":
                continue;
            (src, dest) = self.extract_nodes(line);
            self.add_node(src, "src");
            self.add_node(dest, "dest");
            self.add_edge(src, dest);
        self.write_data();
        self.close_files();
        return;


if __name__ == "__main__":
    bs = BasicExtraction(sys.argv[1], ".", ".");
    bs.process();
