'''
Let's keep all constants here to simpliy the task!
'''

DEFAULT_CLUSTER_ID = -1# initial cluster id that every node in creation should get. it simply means this node does not belong to any cluster
CORE = 1
BORDER = 2
SHELL = 3
IS_BIDIRECTIONAL = False# it is undirectional


def getMax(array ,Lin):
    result = -1
    maxNode = -1
    maxL = -1
    for x in range(len(array)):
        if (array[x][0] >= maxL and array[x][1]>Lin):
            if (array[x][0] == maxL):
                if (int(array[x][3]) > int(maxNode) and maxNode != -1):
                    continue
            result = x
            maxNode = array[x][3]
            maxL = array[x][0]
    return result