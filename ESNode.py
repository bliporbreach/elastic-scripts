from elasticsearch import Elasticsearch, ElasticsearchException


class ESNode:
    """
    ESNode class defines some useful variables and methods for Elasticsearch nodes
    """

    # Constructor takes cluster url, id of the node, and authentication credentials
    def __init__ (self, url, node_id, username, password):
        self.url = url
        self.node_id = node_id
        self.username = username
        self.password = password
        self.cluster_name = None
        self.name = None
        self.host = None
        self.thread_refresh_reject = None
        self.thread_search_reject = None
        self.thread_watcher_reject = None
        self.thread_write_reject = None
        self.jvm_heap_used_percent = None
        self.jvm_heap_max_in_bytes = None
        self.get_stats()

    # Method to return an elasticsearch instance to connect to
    def connect(self):
        es = Elasticsearch(
            [self.url],
            http_auth=(self.username, self.password),
            scheme="https",
        )
        return es

    # Connect to the elasticsearch instance and get stats for the specified node
    def get_stats(self):
        print(f"Getting cluster health info for node {self.node_id}... ")
        try:
            stats = self.connect().nodes.stats(self.node_id)
        except ConnectionError:
            print(f"ERROR: Unable to connect to the cluster {self.alias}")
            exit(1)
        except ElasticsearchException as ex:
            print(f"ERROR: Unable to connect to the cluster {self.alias}\nReason: {ex}")
            exit(1)
        self.cluster_name = stats['cluster_name']
        self.name = stats['nodes'][self.node_id]['name']
        self.host = stats['nodes'][self.node_id]['host']
        self.thread_refresh_reject = stats['nodes'][self.node_id]['thread_pool']['refresh']['queue']
        self.thread_search_reject = stats['nodes'][self.node_id]['thread_pool']['search']['queue']
        self.thread_watcher_reject = stats['nodes'][self.node_id]['thread_pool']['watcher']['queue']
        self.thread_write_reject = stats['nodes'][self.node_id]['thread_pool']['write']['queue']
        self.jvm_heap_used_percent = stats['nodes'][self.node_id]['jvm']['mem']['heap_used_percent']
        self.jvm_heap_max_in_bytes = stats['nodes'][self.node_id]['jvm']['mem']['heap_max_in_bytes']

