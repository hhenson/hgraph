
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect('localhost')