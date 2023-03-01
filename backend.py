from heapq import *
from dataclasses import dataclass
import redis
import struct
import multiprocessing as mp
from config import *

@dataclass
class Entry:
    key: int
    ingress_global: int
    enqueue: int 
    enqueue_delta: int 
    packet_length: int
    saddr: int
    daddr: int
    sport: int
    dport: int

entries = {}

# keys: list, ts_data: dict, ipg_data: dict, psize_data: dict, ts_norm_data: dict

def connect_and_process(key = 0):
    r = redis.Redis(host='localhost', port=6379, db=0)
    print("Connected to Redis")
    keys = r.keys()
    print("Found keys: {}".format(keys))
    print("Extracting data for key: {}".format(key))
    raw_entries = r.zrange(key, 0, -1, withscores=True)
    for raw_entry in raw_entries:
        ts_data_hex = bytes.fromhex(bytes.decode(raw_entry[0]))
        raw_tuple = struct.unpack('>QIIHIIIHH', ts_data_hex)
        # print(raw_tuple)
        entry = Entry(raw_tuple[4], raw_tuple[0], raw_tuple[1], raw_tuple[2], raw_tuple[3], raw_tuple[5], raw_tuple[6], raw_tuple[7], raw_tuple[8])
        if entry.key in entries:
            entries[entry.key].append(entry)
        else:
            entries[entry.key] = [entry]

    return entries

def process_key(key, r):
    entries[key] = {}
    raw_entries = r.zrange(key, 0, -1, withscores=True)
    print("Queried key {}, total entries= {}".format(key, len(raw_entries)))
    for raw_entry in raw_entries:
        ts_data_hex = bytes.fromhex(bytes.decode(raw_entry[0]))
        raw_tuple = struct.unpack('>QIIHIIIHH', ts_data_hex)
        # print(raw_tuple)
        entry = Entry(raw_tuple[4], raw_tuple[0], raw_tuple[1], raw_tuple[2], raw_tuple[3], raw_tuple[5], raw_tuple[6], raw_tuple[7], raw_tuple[8])
        if entry.key in entries[key]:
            entries[key][entry.key].append(entry)
        else:
            entries[key][entry.key] = [entry]
    print("Finished processing key {}".format(key))

def connect_and_process_all_keys(exclude = []):
    r = redis.Redis(host='localhost', port=6379, db=0)
    print("Connected to Redis")
    keys = r.keys()
    print("Found keys: {}".format(keys))
    for key in keys:
        if key in exclude:
            continue
        process_key(key, r)
    return entries

    
## process raw timestamps per flow
def get_ts_data(entries: dict):
    ts_data = {}
    for flow, timestamps in entries.items():
        ts = []
        for entry in timestamps:
            ts.append(entry.ingress_global)
        ts_data[flow] = ts
    return ts_data

## process raw timestamps per flow
def get_ts_data_all_keys(entries_all: dict):
    ts_data = {}
    for key, entries in entries_all.items():
        ts_data[key] = {}
        for flow, timestamps in entries.items():
            ts = []
            for entry in timestamps:
                ts.append(entry.ingress_global)
            ts_data[key][flow] = ts
    return ts_data

## process packet sizes per flow
def get_ts_sizes(entries: dict):
    size_data = {}
    for flow, timestamps in entries.items():
        ts = []
        for entry in timestamps:
            ts.append(entry.packet_length)
        size_data[flow] = ts
    return size_data

## process packet sizes per flow
def get_ts_sizes_all_keys(entries_all: dict):
    size_data = {}
    for key, entries in entries_all.items():
        size_data[key] = {}
        for flow, timestamps in entries.items():
            ts = []
            for entry in timestamps:
                ts.append(entry.packet_length)
            size_data[key][flow] = ts
    return size_data

## process packet queueing delays per flow
def get_queue_delta_all_keys(entries_all: dict):
    delta_data = {}
    for key, entries in entries_all.items():
        delta_data[key] = {}
        for flow, timestamps in entries.items():
            delta = []
            for entry in timestamps:
                delta.append(entry.enqueue_delta)
            delta_data[key][flow] = delta
    return delta_data


## process flow information per flow
def get_flowinfo(entries: dict):
    flow_info = {}
    for flow, timestamps in entries.items():
        flow_4tuple = (timestamps[0].saddr, timestamps[0].daddr, timestamps[0].sport, timestamps[0].dport)
        flow_info[flow] = flow_4tuple
    return flow_info

## process flow information per flow
def get_flowinfo(entries_all: dict):
    flow_info = {}
    for key, entries in entries_all.items():
        flow_info[key] = {}
        for flow, timestamps in entries.items():
            flow_4tuple = (timestamps[0].saddr, timestamps[0].daddr, timestamps[0].sport, timestamps[0].dport)
            flow_info[key][flow] = flow_4tuple
    return flow_info

## process inter-packet-gaps per flow
def get_ipg_data(entries: dict):
    ipg_data = {}
    ts_norm_data = {}
    for flow, timestamps in entries.items():
        ipg = []
        normed = []
        first_entry_t0 = timestamps[0].ingress_global
        for entry in range(1, len(timestamps)):
            t1_t = timestamps[entry - 1]
            t2_t = timestamps[entry]
            gap = t2_t.ingress_global - t1_t.ingress_global
            if gap < INVALID_THRESHOLD:
                ipg.append(gap)
            normed.append(t2_t.ingress_global - first_entry_t0)
        ipg_data[flow] = ipg
        ts_norm_data[flow] = normed
    return ipg_data, ts_norm_data

## process inter-packet-gaps per flow
def get_ipg_data_all_keys(entries_all: dict):
    ipg_data = {}
    ts_norm_data = {}
    for key, entries in entries_all.items():
        ipg_data[key] = {}
        ts_norm_data[key] = {}
        for flow, timestamps in entries.items():
            ipg = []
            normed = []
            first_entry_t0 = timestamps[0].ingress_global
            for entry in range(1, len(timestamps)):
                t1_t = timestamps[entry - 1]
                t2_t = timestamps[entry]
                gap = t2_t.ingress_global - t1_t.ingress_global
                if gap < INVALID_THRESHOLD:
                    ipg.append(gap)
                normed.append(t2_t.ingress_global - first_entry_t0)
            ipg_data[key][flow] = ipg
            ts_norm_data[key][flow] = normed
    return ipg_data, ts_norm_data

def get_aggregate_ts_data(entries: dict):
    ts_data = {0: []}
    temp_data = []
    for _, timestamps in entries.items():
        for entry in timestamps:
            heappush(temp_data, entry.ingress_global)
    for _ in range(len(temp_data)):
        ts_data[0].append(heappop(temp_data))
    return ts_data

## process aggregated raw timestamps 
def get_aggregate_ts_data_all_keys(entries_all: dict):
    ts_data = {}
    temp_data = {}
    for key, entries in entries_all.items():
        ts_data[key] = []
        temp_data[key] = []
        for _, timestamps in entries.items():
            for entry in timestamps:
                heappush(temp_data[key], entry.ingress_global)
        for _ in range(len(temp_data[key])):
            ts_data[key].append(heappop(temp_data[key]))
    return ts_data

## process aggregated packet sizes
def get_aggregate_ts_sizes(entries: dict):
    raise NotImplementedError("Not neccessary for now")
    
## process aggregated packet sizes
def get_aggregate_ts_sizes_all_keys(entries_all: dict):
    size_data = {}
    temp_data = {}
    for key, entries in entries_all.items():
        size_data[key] = []
        temp_data[key] = []
        for _, timestamps in entries.items():
            for entry in timestamps:
                heappush(temp_data[key], (entry.ingress_global, entry.packet_length))
        for _ in range(len(temp_data[key])):
            size_data[key].append(heappop(temp_data[key])[1])
    return size_data

## process aggregated inter-packet-gaps
def get_aggregate_ipg_data(entries: dict):
    ts_data = get_aggregate_ts_data(entries)[0]
    ipg_data = {0: []}
    ts_norm_data = {0: []}
    first_entry_t0 = ts_data[0]
    cur = 0
    prev = first_entry_t0
    for entry in range(1, len(ts_data)):
        cur = ts_data[entry]
        gap = cur - prev
        if gap < INVALID_THRESHOLD:
            ipg_data[0].append(gap)
        ts_norm_data[0].append(cur - first_entry_t0)
        prev = cur
    return ipg_data, ts_norm_data

## process aggregated inter-packet-gaps
def get_aggregate_ipg_data_all_keys(entries_all: dict):
    ts_data_all = get_aggregate_ts_data_all_keys(entries)
    ipg_data = {}
    ts_norm_data = {}
    for key, ts_data in ts_data_all.items():
        ipg_data[key] = []
        ts_norm_data[key] = []
        first_entry_t0 = ts_data[0]
        cur = 0
        prev = first_entry_t0
        for entry in range(1, len(ts_data)):
            cur = ts_data[entry]
            gap = cur - prev
            if gap < INVALID_THRESHOLD:
                ipg_data[key].append(gap)
            ts_norm_data[key].append(cur - first_entry_t0)
            prev = cur
    return ipg_data, ts_norm_data

def GetMaxRecords(ts_list):        
    max_record = max(ts_list, key=lambda k: len(ts_list[k]))
    return len(ts_list[max_record]), max_record

def GetMinRecords(ts_list):        
    min_record = min(ts_list, key=lambda k: len(ts_list[k]))
    return len(ts_list[min_record]), min_record
