from elasticsearch import Elasticsearch, ElasticsearchException
from sys import exit


class ESCluster:
    """
    ESCluster class defines some useful variables and methods for Elasticsearch clusters
    """

    # Constructor takes cluster alias, id of the node, and authentication credentials
    def __init__(self, alias, url, username, password):
        self.alias = alias
        self.url = url
        self.username = username
        self.password = password
        self.status = None
        self.name = None
        self.nodes = None
        self.active_shards = None
        self.unassigned_shards = None
        self.auto_create_index = None
        self.display_name = None
        self.index_count = None
        self.doc_count = None
        self.node_count = None
        self.data_node_count = None
        self.ingest_node_count = None
        self.master_node_count = None
        self.ml_node_count = None
        self.coordinating_only_count = None
        self.jvm_heap_max_in_bytes = None
        self.jvm_heap_used_in_bytes = None
        self.jvm_heap_used_ratio = None
        self.jvm_threads = None
        self.processors = None
        self.mem_used_percent = None
        self.get_health()
        self.get_settings()
        self.get_stats()

    # Method to return an elasticsearch instance to connect to
    def connect(self):
        es = Elasticsearch(
            [self.url],
            http_auth=(self.username, self.password),
            scheme="https",
        )
        return es

    # Method to connect to the elasticsearch cluster and get health metrics
    def get_health(self):
        print(f"Getting cluster health info for {self.alias}... ")
        try:
            health = self.connect().cluster.health()
        except ConnectionError:
            print(f"ERROR: Unable to connect to the cluster {self.alias}")
            exit(1)
        except ElasticsearchException as ex:
            print(f"ERROR: Unable to connect to the cluster {self.alias}\nReason: {ex}")
            exit(1)
        self.name = health['cluster_name']
        self.status = health['status']
        self.nodes = health['number_of_nodes']
        self.active_shards = health['active_shards']
        self.unassigned_shards = health['unassigned_shards']

    # Method to connect to the elasticsearch cluster and get settings
    def get_settings(self):
        print(f"Getting cluster settings for {self.alias}... ")
        try:
            settings = self.connect().cluster.get_settings()
        except ConnectionError:
            print(f"ERROR: Unable to connect to the cluster {self.alias}")
        self.auto_create_index = settings['persistent']['action']['auto_create_index']
        self.display_name = settings['persistent']['cluster']['metadata']['display_name']

    # Method to connect to the elasticsearch cluster and get stats
    def get_stats(self):
        print(f"Getting cluster stats for {self.alias}... ")
        try:
            stats = self.connect().cluster.stats()
        except ConnectionError:
            print(f"ERROR: Unable to connect to the cluster {self.alias}")
        self.index_count = stats['indices']['count']
        self.doc_count = stats['indices']['docs']['count']
        self.node_count = stats['nodes']['count']['total']
        self.data_node_count = stats['nodes']['count']['data']
        self.ingest_node_count = stats['nodes']['count']['ingest']
        self.master_node_count = stats['nodes']['count']['master']
        self.ml_node_count = stats['nodes']['count']['ml']
        self.coordinating_only_count = stats['nodes']['count']['coordinating_only']
        self.jvm_heap_max_in_bytes = stats['nodes']['jvm']['mem']['heap_max_in_bytes']
        self.jvm_heap_used_in_bytes = stats['nodes']['jvm']['mem']['heap_used_in_bytes']
        self.jvm_heap_used_ratio = self.jvm_heap_used_in_bytes / self.jvm_heap_max_in_bytes
        self.jvm_threads = stats['nodes']['jvm']['threads']
        self.processors = stats['nodes']['os']['available_processors']
        self.mem_used_percent = stats['nodes']['os']['mem']['used_percent']

    # Method to connect to the elasticsearch cluster and get a list of all nodes
    def get_nodes(self):
        print(f"Enumerating nodes in cluster {self.alias}...")
        return self.connect().nodes.info()['nodes'].keys()

