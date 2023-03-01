import os
import sys
module_path = os.path.abspath(os.path.join('..'))
if module_path not in sys.path:
    sys.path.append(module_path)
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import ScalarFormatter
from statsmodels.distributions.empirical_distribution import ECDF
from backend import *


entries = {}
PER_FLOW_FILES = False
FLOWLET_THRESH = 1000

def aggregate_and_create_files(entries_all: dict, threshold, exp):
    size_data = {}
    temp_data = {}
    for key, entries in entries_all.items():
        size_data[key] = []
        temp_data[key] = []
        flowlet_ctr = 1
        flowlet_hash = {}
        flowlet_index = [0]
        for _, timestamps in entries.items():
            for entry in timestamps:
                heappush(temp_data[key], (entry.ingress_global, entry.packet_length, entry.key))
        with open("redis_{}_{}.csv".format(exp, key), "w") as f:
            for _ in range(len(temp_data[key])):
                item = heappop(temp_data[key])
                
                ## flowlet handling
                if item[2] in flowlet_hash:
                    if item[0] - flowlet_hash[item[2]][1] <= threshold:
                        fid = flowlet_hash[item[2]][0]
                        flowlet_index[fid] += 1
                        flowlet_hash[item[2]] = (flowlet_hash[item[2]][0], item[0])
                    else:
                        fid = flowlet_ctr
                        flowlet_hash[item[2]] = (flowlet_ctr, item[0])
                        flowlet_index.append(1)
                        flowlet_ctr += 1
                    
                else:
                    fid = flowlet_ctr
                    flowlet_hash[item[2]] = (flowlet_ctr, item[0])
                    flowlet_index.append(1)
                    flowlet_ctr += 1
                
                ####
                f.write("{},{},{}\n".format(fid, item[0], item[1]))
        with open("redis_{}_{}_flowlet_index.csv".format(exp, key), "w") as f:
            for i in range(1, len(flowlet_index)):
                f.write("{},{}\n".format(i, flowlet_index[i]))
        print("Created file for key {}".format(key))
        print("{} flowlets detected for key={}, with threshold = {}ns".format(flowlet_ctr - 1, key, threshold))

          
def process_key(key, r):
    entries[key] = {}
    s_ports = {}
    raw_entries = r.zrange(key, 0, -1, withscores=True)
    print("Queried key {}, total entries= {}".format(key, len(raw_entries)))
    for raw_entry in raw_entries:
        ts_data_hex = bytes.fromhex(bytes.decode(raw_entry[0]))
        raw_tuple = struct.unpack('>QIIHIIIHH', ts_data_hex)
        entry = Entry(raw_tuple[4], raw_tuple[0], raw_tuple[1], raw_tuple[2], raw_tuple[3], raw_tuple[5], raw_tuple[6], raw_tuple[7], raw_tuple[8])
        s_ports[entry.sport] = 1
        if entry.key in entries[key]:
            entries[key][entry.key].append(entry)
        else:
            entries[key][entry.key] = [entry]
    if PER_FLOW_FILES:
        for key, flow in entries.items():
            for flow_id, ts_list in flow.items():
                with open("redis_{}_{}.csv".format(key, flow_id), "w") as f:
                    for ts in ts_list:
                        f.write("{},{}\n".format(ts.ingress_global, ts.packet_length))
        for sport, v in s_ports.items():
            print(sport)
    print("Finished processing key {}".format(key))

def connect_and_convert_to_csv(exp, exclude = []):
    r = redis.Redis(host='localhost', port=6379, db=0)
    print("Connected to Redis")
    keys = r.keys()
    print("Found keys: {}".format(keys))
    for key in keys:
        if key in exclude:
            continue
        process_key(key, r)
    aggregate_and_create_files(entries, 10000, exp)
    return entries
          
    
if __name__ == '__main__':
    exp = 0
    if len(sys.argv) == 2:
        exp = int(sys.argv[1])
    connect_and_convert_to_csv(exp, [])