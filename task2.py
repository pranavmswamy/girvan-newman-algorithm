from pyspark import SparkContext
from sys import argv
from time import time

sc = SparkContext()
sc.setLogLevel("ERROR")
start_time = time()

power_file_rdd = sc.textFile("sample.txt").map(lambda x: (x.split(" ")[0], x.split(" ")[1]))
edges_rdd = power_file_rdd.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])]).distinct()
print("Number of edges = ", edges_rdd.count()/2)
adj_list = edges_rdd.groupByKey().mapValues(lambda l: list(l))
network = adj_list.collectAsMap()
# print(adj_list.count())
print(adj_list.collect())


def betweenness_trial(root, dicTree):
    # step 1: find the bfs tree graph
    # root = x[0]
    head = root
    bfsTraversal = [head]  # the list of bfs traversal order
    nodes = set(bfsTraversal)
    treeLevel = {root: 0}  # {node: level}
    node_values = {root: 1}
    dicParent = {}  # {childNode: [parent nodes in bfs graph]}
    dicChild = {} # {node: [children]}
    index = 0  # the current index of the bfsTraversal
    while index < len(bfsTraversal):
        head = bfsTraversal[index]
        node_values[head] = 0
        if head == root:
            node_values[head] = 1
        else:
            for parent in dicParent[head]:
                node_values[head] += node_values[parent]

        children = dicTree[head]
        for child in children:
            if child not in nodes:
                bfsTraversal.append(child)
                nodes.add(child)
                treeLevel[child] = treeLevel[head] + 1
                dicParent[child] = [head]
                if head in dicChild:
                    dicChild[head].append(child)
                else:
                    dicChild[head] = [child]
            else:
                if treeLevel[child] == treeLevel[head] + 1:
                    dicParent[child] += [head]
                    if head in dicChild:
                        dicChild[head].append(child)
                    else:
                        dicChild[head] = [child]
        index += 1

    #print("------------------------------")
    #print("Root Node: ", root)
    #print("Tree Level Dict: ", treeLevel)
    #print("Node values Dict: ", node_values)
    #print("Parent Dict: ", dicParent)
    #print("Child Dict: ", dicChild)

    # step 2: calculate betweenness for each bfs graph

    credit_list = []
    dicNodeWeight = {}
    for i in range(len(bfsTraversal) - 1, -1, -1):
        node = bfsTraversal[i]
        if node not in dicNodeWeight:
            dicNodeWeight[node] = 1.0
        if node in dicParent:
            if node in dicChild:
                parents = dicParent[node]
                parentsSize = len(parents)
                for parent in parents:
                    addition = float(dicNodeWeight[node]) / parentsSize
                    if parent not in dicNodeWeight:
                        dicNodeWeight[parent] = 1.0
                    dicNodeWeight[parent] += addition
                    tmpEdge = (min(node, parent), max(node, parent))
                    credit_list.append((tmpEdge, addition))
            else: # node not in dicChild # leaf node
                parents = dicParent[node]
                for parent in parents:
                    addition = node_values[parent] / node_values[node]
                    if parent not in dicNodeWeight:
                        dicNodeWeight[parent] = 1.0
                    dicNodeWeight[parent] += addition
                    tmpEdge = (min(node, parent), max(node, parent))
                    credit_list.append((tmpEdge, addition))
    return credit_list

betweenness_rdd = adj_list.flatMap(lambda node: betweenness_trial(node[0], network)).groupByKey().mapValues(lambda l: sum(l)/2) # returns [((a,b), val), ... ]
print(betweenness_rdd.collect())
print("Betweenness edges - ", betweenness_rdd.count())
# YAAAAY! Betweenness done!!!

def modularity(orig_network, network_components, m):
    '''

    :param orig_network: dict of original network
    :param network_components: list of dict of component networks
    :param m: number of edges in original_network
    :return: modularity [-1, 1]
    '''





print("Time taken: ", time() - start_time, "s")


