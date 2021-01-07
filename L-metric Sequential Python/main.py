from constants import *
from Graph import Graph
from Node import Node
from LMetric import LMetric
import sys

def create_graph(input_file_name):
	input_file = open(input_file_name,'r')
	my_graph = Graph(IS_BIDIRECTIONAL)

	temp_source_node_id = -1
	for line in input_file:
		line = line.strip()
		if 'id ' in line:
			n_id = line.split()[-1]
			node = Node(n_id)
			my_graph.add_node(node)
		elif 'source' in line:
			temp_source_node_id = line.split()[-1]
		elif 'target' in line:
			temp_target_node_id = line.split()[-1]
			my_graph.add_edge(temp_source_node_id,temp_target_node_id)
		else:
			pass
	input_file.close()
	return my_graph

case=1
output_file_name = 'out.3'
if case==1:
	input_file_name = 'karate/karate.gml'
	my_g = create_graph(input_file_name)
	print(my_g)
	# now lets evaluate communities inside it.
	lm = LMetric(my_g)
	lm.run_lmetric_algorithm(output_file_name)


elif case==2:
	g2 = Graph(False)
	n1= Node('1')
	n2= Node('2')
	n3= Node('3')
	n4= Node('4')
	n5= Node('5')
	n6= Node('6')
	n7= Node('7')
	n8= Node('8')

	g2.add_node(n1)
	g2.add_node(n2)
	g2.add_node(n3)
	g2.add_node(n4)
	g2.add_node(n5)
	g2.add_node(n6)
	g2.add_node(n7)
	g2.add_node(n8)

	g2.add_edge('1','2')
	g2.add_edge('1','3')
	g2.add_edge('1','4')
	g2.add_edge('2','3')
	g2.add_edge('2','4')
	g2.add_edge('3','4')
	g2.add_edge('4','5')
	g2.add_edge('5','6')
	g2.add_edge('6','7')
	g2.add_edge('6','8')
	g2.add_edge('7','8')

	print(g2)
	lm = LMetric(g2)
	lm.run_lmetric_algorithm(output_file_name)
