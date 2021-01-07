from constants import *
from Node import Node

class Graph:

	def __init__(self,is_bidirectional):
		self.__nodes = dict()
		self.__is_directional = is_bidirectional

	def add_node(self, node):
		if node.get_node_id() in self.__nodes:
			raise Exception('node with this id already exists...')
		else:
			self.__nodes[node.get_node_id()] = node

	def add_edge(self,source, target):
		if self.__is_directional:
			if source not in self.__nodes:
				raise Exception('node id is not found while adding an edge for it. it is the source')
			elif target not in self.__nodes:
				raise Exception('node id is not found while adding an edge for it. it is the target')
			else:
				self.__nodes[source].set_edges(target)
		else:
			if source not in self.__nodes:
				raise Exception('node id is not found while adding an edge for it. it is the source')
			elif target not in self.__nodes:
				raise Exception('node id is not found while adding an edge for it. it is the target')
			else:
				# print('ADDITION:', source,target)
				self.__nodes[source].set_edges(target)
				self.__nodes[target].set_edges(source)

	def remove_node(self,node_id):
		print('****** current state of graph is:',self.__nodes)
		node_id = str(node_id)
		if node_id not in self.__nodes:
			raise Exception('id to be removed was not found in the graph: ', node_id)
		else:
			neighbours = self.__nodes[node_id].get_edges()
			for neighbour_id in neighbours:
				self.__nodes[neighbour_id].remove_edge(node_id)
			del self.__nodes[node_id]

	def get_node_using_id(self,node_id):
		if node_id not in self.__nodes:
			raise Exception('id not found in the graph, in get_node_using_id:', node_id)
		else:
			return self.__nodes[node_id]

	def get_nodes(self):
		return sorted([key for (key,val) in self.__nodes.items()],key= lambda x:int(x))

	def has_node(self, node_id):
		if node_id in self.__nodes:
			return True
		return False

	def get_edges(self,node_id):
		if node_id not in self.__nodes:
			raise Exception('node not found in the graph, get_edges:',node_id)
		else:
			return self.__nodes[node_id].get_edges()

	def __len__(self):
		return len(self.__nodes)
	
	def __str__(self):
		ret = ''
		for key,node in self.__nodes.items():
			#edges_str = ','.join(node.get_edges())
			#ret += str(node.get_node_id()) + ': '+ edges_str + '\n'
			ret += str(node) + '\n'
		return ret
			