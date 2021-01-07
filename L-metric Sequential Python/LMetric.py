from constants import *
from Graph import Graph
import copy

'''
Cluster --> ID of nodes that are in the community (Union of CORE and BORDER)
D = union CORE and BORDER
S --> SHELL
'''
class LMetric:
	def __init__(self, graph):
		self.__graph = graph
		print('size of the graph is:',len(graph))

	## probable error: IDs are sometimes int and sometimes integer!
	'''
	get L_in: iterate over all nodes in the cluster, get number of edges that are to other nodes in the cluster
	get L_ex: iterate over all nodes in the cluster that have a link to out (=are border) and count the sum of outdegrees
	=> cluster should be very very up to date!
	'''
	def evaluate_community(self, cluster):
		boundary_size = 0
		lmetric = 0.0
		l_in = 0
		l_ex = 0
		a=set()
		a.add('1')
		a.add('13')
		a.add('4')

		if (cluster == a):
			a=1
		if len(cluster)>1:
			for node in cluster:
				l_in += self.calculate_inner_degree(self.__graph.get_node_using_id(node),cluster)

				out_degree = self.calculate_outer_degree(self.__graph.get_node_using_id(node),cluster)
				if out_degree!=0: # it is a border node
					boundary_size+=1
					l_ex+=out_degree
			l_in = l_in/len(cluster)

			if boundary_size!=0:
				l_ex = l_ex/boundary_size
				#l_ex = l_ex/len(cluster)
				lmetric = l_in/l_ex
			else:
				lmetric = l_in
			# if normalize ---> not implemented yet!
		return lmetric,l_in,l_ex

	# return number of neighbors node has in the cluster
	def calculate_inner_degree(self, node,cluster):
		count = 0
		for neighbor_node_id in self.__graph.get_edges(node.get_node_id()):
			if neighbor_node_id in cluster:
				count +=1
		return count

	# return number of neighbors node has in the shell (ie not in the cluster)
	def calculate_outer_degree(self, node,cluster):
		count = 0
		for neighbor_node_id in self.__graph.get_edges(node.get_node_id()):
			if neighbor_node_id not in cluster:
				count +=1
		return count



	def discovery_phase(self,n0):
		# some initializations, including setting the starting node, adding to the community and getting the initial shell
		n0.set_cluster_id(CORE)
		community = set()
		Shell = set()
		community.add(n0.get_node_id())
		for node_id in n0.get_edges():
			#self.__graph.get_node_using_id(node_id).set_cluster_id(SHELL)
			Shell.add(node_id)
		# iterating to add new nodes to the community, till no more can be added
		while True:
			# getting the very initial L values
			print('start of new iteration. community is:', sorted(community,key=lambda x:int(x)),'Shell is:',sorted(Shell,key=lambda x:int(x)))
			L,L_in,L_ex = self.evaluate_community(community)
			new_Ls  = []
			print('Current L:',L)
			for node_id in Shell:
				if node_id in community:
					raise Exception("Node thought to be a shell node is already in the cluster:",node_id)
				community.add(node_id)
				#self.__graph.get_node_using_id(node_id).set_cluster_id(BORDER)## border or core???
				stat0,stat1,stat2 = self.evaluate_community(community)
				new_Ls.append((stat0,stat1,stat2,node_id))
				community.remove(node_id)
				print('checking addition of this node to the community:', node_id,'results are:', stat0,'L_in:',stat1,'current L measure is:',L,'org_L_in:',L_in)
				#self.__graph.get_node_using_id(node_id).set_cluster_id(SHELL)
			
			# sort the L_primes, to find the first one that is in the first or the third cases.
			new_Ls.sort(key= lambda x:x[3])
			new_Ls.sort(key= lambda x:x[0], reverse=False)# makhraj kuchiktar olaviat bishtar
			new_Ls.sort(key= lambda x:x[1], reverse=True)

			finished = False
			added = False
			for i in range(len(new_Ls)):
				#if new_Ls[i][0]<=L:
				#	print('No progress, The End!')
				#	finished = True
				#	break
				maxIndex= getMax(new_Ls, L_in)
				if new_Ls[maxIndex][0]<L:
					break
				if new_Ls[maxIndex][1]>L_in:# case one and case 3
					community.add(new_Ls[maxIndex][3])# adding the node to the community
					Shell.remove(new_Ls[maxIndex][3])
					for node_id in self.__graph.get_node_using_id(new_Ls[maxIndex][3]).get_edges():
						if node_id not in community:
							Shell.add(node_id)
					print('This node is added to the community:',new_Ls[maxIndex][3])
					added = True
					break
				del new_Ls[maxIndex]
					#print('case 2 for:',new_Ls[i][3])
				#self.__graph.get_node_using_id(new_Ls[i][3]).set_cluster_id(BORDER)## border or core???
				
			if not added:
				print('No progress, The End!')
				finished = True
				
				
			'''
			update_B()
			update_S()
			update_C()
			update_L()
			'''
			if finished or len(new_Ls)==0:## should this be before updates or after them?
				# finish procedure
				print('initial local community detected:',sorted(community,key=lambda x:int(x)))
				return community,n0.get_node_id()





	def examination_phase(self,cluster):
		# computer L_in and L_out
		org_l,org_l_in,org_l_ex = self.evaluate_community(cluster)
		print('original values are:',org_l,org_l_in,org_l_ex)
		to_be_removed = []
		cluster_copy = copy.copy(cluster)
		for node_id in cluster_copy:
			# remove node_id from cluster
			# and compute Lp_in and Lp_out
			# compare with the L_in and L_out
			# keep only if in the third case
			cluster.remove(node_id)
			new_l,new_l_in,new_l_ex = self.evaluate_community(cluster)
			print('original:',org_l,org_l_in,org_l_ex,'with removal of:',node_id,':',new_l,new_l_in,new_l_ex)
			if not(new_l_ex>=org_l_ex):# denom should get smaller and numerator
				print('removing:',node_id,'ORG_IN:',org_l_in,'new_in:',new_l_in,'ORG_EX:',org_l_ex,'new_ex:',new_l_ex)
				# must be removed, keeping its id to removing it at the end!
				to_be_removed.append(node_id)
			cluster.add(node_id)
		for node_id_ind in range(len(to_be_removed)):
			print('lets remove:',to_be_removed[node_id_ind],'ftom',cluster)
			cluster.remove(to_be_removed[node_id_ind])
		print('these nodes are removed:',sorted(to_be_removed,key=lambda x:int(x)))
		return cluster

	def is_start_node_in_the_community(self,community,start_node_id):
		if start_node_id in community:
			return True
		return False

	def run_lmetric_algorithm(self,output_file_name):
		'''
		# getting id of the starting node from user
		while True:	
			temp = input('please provide id of a node of the graph: ')
			if not temp.isnumeric():
				print('Incorrect.')		
				continue
				#temp = int(temp)
			if self.__graph.has_node(temp):
				print('node with this ID is found!')
				#self.__start_node_id = temp
				n0 = self.__graph.get_node_using_id(temp)
				break
		'''
		output_file = open(output_file_name,'a')
		output_file.write('\n')
		remaining = self.__graph.get_nodes()

		ind = 0
		remainingCntr=0
		finished = False
		while finished==False:
			sorted(remaining, key=lambda x: int(x))
			print('\t\tCurrent remaining:',remaining)
			if remainingCntr>=len(remaining):
				print('reached the end!, terminating')
				break

			n0 = self.__graph.get_node_using_id( remaining[ind])
			community,start_node_id = self.discovery_phase(n0)
			
			#community_pruned = community
			#orig_community=copy.deepcopy(community)
			community_pruned = self.examination_phase(community)
			diff = community.difference(community_pruned)
			if self.is_start_node_in_the_community(community_pruned,start_node_id):
				print('community is:',sorted(community_pruned,key=lambda x:int(x)))
				output_file.write('Community detected:'+str(sorted(community_pruned,key=lambda x:int(x)))+'\n')
				for node_id in community:
					self.__graph.remove_node(node_id)


					if node_id in remaining:
						del remaining[remaining.index(node_id)]

					if len(remaining)>0:
						if (len(diff)>0):
							sorted(diff, key=lambda x: int(x))
							for firstRemovedElement in diff:
								break
							#ind = remaining.index(firstRemovedElement)
							ind=0
						else:
							ind = 0
						remainingCntr=0
					else:
						print( "FINISH!")
						finished = True
						break
			else:
				print('community does not exist since start node is not inside it.')
				if len(remaining)>1:
					ind = (ind+1) % len(remaining)
					remainingCntr+=1
				else:
					print( "FINISH")
					finished = True
					break
		output_file.write('no Community:'+str(remaining)+'\n')
		output_file.close()

