from constants import *

class Node:
	'''
	for each node, lets keep its id, and cluster_id,\
	 and list of neighbor vertices (only teir IDs) in the edges list
	'''
	def __init__(self, node_id):
		self.__node_id = str(node_id)
		self.__edges = []
		self.__cluster_id = DEFAULT_CLUSTER_ID
		self.__all_cluster_ids = set()# in every iteration, it may get a new id // unclear for now if it will be used!

	def get_node_id(self):
		return str(self.__node_id)

	def get_edges(self):
		return self.__edges

	def remove_edge(self,id):
		del self.__edges[self.__edges.index(id)]

	def set_edges(self,edges):
		if len(edges)==0:
			return
		self.__edges.append(edges)

	def get_cluster_id(self):
		return self.__cluster_id

	def set_cluster_id(self,cluster_id):
		if not str(cluster_id).isnumeric():
			raise Exception('cluster id is not an integer')
		self.__cluster_id = cluster_id

	def get_edges_size(self):
		return len(self.__edges)

	def __str__(self):
		return 'node id: ' + str(self.__node_id) + ' edges are: '+ str(' - '.join(self.__edges))
