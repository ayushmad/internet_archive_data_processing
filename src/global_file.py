import os

DATA_DIR_109 = "../input_files/109/*"
DATA_DIR_110 = "../input_files/110/*"
DATA_DIR_111 = "../input_files/111/*"
DATA_DIR_112 = "../input_files/112/*"


# Creating Temp Dir
TEMP_DIR = "../temp/"
OUTPUT_DIR = "../output_files/"
DELIMITER=";"

INPUT_FILES_DIR = "../input_files/";
INTERIM_DIR = "../interm_files/";
BASIC_NODES = "basic_nodes";
BASIC_EDGES = "basic_edges";
MERGE_NODES = "merge_nodes";
MERGE_EDGES = "merge_edges";
CCTLD_FILE = "../lib/static_assets/cctld_list.dat";
TLD_FILE = "../lib/static_assets/tld.dat";
GEOIP_MAP = "../lib/static_assets/GeoIP.dat";
DOMAIN_MAPPER_DIR = "domain_mapper";
GEOIP_MAPPER_DIR = "geoip_mapper";
OUT_TEMPLETE = "node_file_[YEAR]";
OUT_YEAR_TEMPELETE = "[YEAR]"

def get_input_dir():
    return INPUT_FILES_DIR;

def get_interm_dir():
    create_if_not_exist(INTERIM_DIR);
    return INTERIM_DIR;

def get_temp_dir():
    create_if_not_exist(TEMP_DIR);
    return TEMP_DIR;

def create_if_not_exist(my_dir):
    if not os.path.exists(my_dir):
        os.makedirs(my_dir);
    return;


def clean_dir(temp_dir):
    if(temp_dir == '/' or  temp_dir== "\\"): return
    else:
        for root, dirs, files in os.walk(temp_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name));
    return;

def move_file(src, dest):
    os.system("mv %s %s"%(src, dest));
    return;

def copy_file(src, dest):
    os.system("cp %s %s"%(src, dest));
    return;
