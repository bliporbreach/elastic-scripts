#!/usr/bin/env python3.8

# Script to print out Elasticsearch cluster health metrics
from argparse import ArgumentParser
from json import load
from getpass import getpass
from sys import exit
from tabulate import tabulate
from classes.ESCluster import ESCluster
from classes.ESNode import ESNode


# Parse commandline arguments
parser = ArgumentParser(description='Test connectivity and perform health check on Elasticsearch clusters')
parser.add_argument('--username', help='username')
parser.add_argument('--password', help='password')
parser.add_argument('--version', '-v', action='version', version='%(prog)s 1.0.2')
args = parser.parse_args()

# Ask for username and password if not provided as an argument
if not args.username:
    username = ''
    while not username:
        username = input("Elasticsearch Username: ")

if not args.password:
    password = ''
    while not password:
        password = getpass()

# Load clusters from a JSON dictionary
try:
    cluster_list = load(open("./clusters.json"))
except FileNotFoundError:
    print("ERROR: clusters.json file not found!")
    exit(1)

# Poll elasticsearch clusters and nodes, store results in lists
clusters = []
nodes = []
for alias, url in cluster_list.items():
    print(f"--------------\n{alias}\n--------------")
    cluster = ESCluster(alias, url, username, password)
    clusters.append({'display_name': cluster.display_name, 'status': cluster.status, 'active\nshards':
                     cluster.active_shards, 'unassigned\nshards': cluster.unassigned_shards,
                     'auto_create\nindex': cluster.auto_create_index, 'index\ncount': cluster.index_count,
                     'doc\ncount': cluster.doc_count, 'node\ncount': cluster.node_count, 'jvm\nmax(gb)':
                         str(round(cluster.jvm_heap_max_in_bytes/1024/1024/1024, 2)), 'jvm\nused(%)':
                         str(round(cluster.jvm_heap_used_ratio*100))+"%", 'jvm\nthreads':
                     cluster.jvm_threads, 'processors': cluster.processors, 'mem\nused(%)':
                         cluster.mem_used_percent})
    for node_id in cluster.get_nodes():
        node = ESNode(url, node_id, username, password)
        nodes.append({'cluster\nalias': alias, 'server\nname': node.name, 'host': node.host,
                      'rejected\nrefresh\nthreads': node.thread_refresh_reject,
                      'rejected\nsearch\nthreads': node.thread_search_reject, 'rejected\nwatcher\nthreads':
                      node.thread_watcher_reject, 'rejected\nwrite\nthreads': node.thread_write_reject,
                      'jvm\nheap\nused(%)': node.jvm_heap_used_percent, 'jvm\nheap\nsize(gb)':
                          str(round(node.jvm_heap_max_in_bytes/1024/1024/1024, 2))})

# Print out the results in table format
print(tabulate(clusters, headers="keys", tablefmt="fancy_grid"))
print(tabulate(nodes, headers="keys", tablefmt="fancy_grid"))


