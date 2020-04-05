from pyspark import SparkContext
from sys import argv
from time import time
from random import choice
from itertools import combinations

sc = SparkContext()
sc.setLogLevel("ERROR")
start_time = time()


# ----------------------------------------------------------------------------------------------------------------------

def get_network_components(old_components, edge_to_break):

    old_network_components = [comp.collectAsMap() for comp in old_components]

    new_components = []
    if edge_to_break is not None:
        # removing that edge
        for component in old_network_components:
            if edge_to_break[0] in component.keys():
                # print("Before removing - ", component[edge_to_break[0]])
                component[edge_to_break[0]].remove(edge_to_break[1])
                # print("After removing - ", component[edge_to_break[0]])
                if len(component[edge_to_break[0]]) == 0:
                    del component[edge_to_break[0]]
                    new_components.append(sc.parallelize([(edge_to_break[0], [])]).persist())
            if edge_to_break[1] in component.keys():
                # print("Before removing - ", component[edge_to_break[1]])
                component[edge_to_break[1]].remove(edge_to_break[0])
                # print("After removing - ", component[edge_to_break[1]])
                if len(component[edge_to_break[1]]) == 0:
                    del component[edge_to_break[1]]
                    new_components.append(sc.parallelize([(edge_to_break[1], [])]).persist())
        # print("(get_network_components) num of Components after removing edge: ", len(old_network_components))
    for component in old_network_components:
        # perform bfs and keep adding RDDs of new components to new_components
        vertices = set(component.keys())
        visited = set()
        # new_components =[]
        remaining_vertices = set(component.keys())
        while remaining_vertices:
            bfsQ = [choice(tuple(remaining_vertices))]
            new_component = [(bfsQ[0], component[bfsQ[0]])]
            while bfsQ:
                node = bfsQ.pop(0)
                visited.add(node)
                children = component[node]
                for child in children:
                    if child not in visited:
                        visited.add(child)
                        bfsQ.append(child)
                        if (node, children) not in new_component:
                            new_component.append((node, children))
                        if (child, component[child]) not in new_component:
                            new_component.append((child, component[child]))
            new_components.append(sc.parallelize(new_component).persist())
            remaining_vertices = vertices.difference(visited)
    return new_components


def get_btw_cent_compnt(root_node, network_component):
    bfsQ = [root_node]
    bfsQ_copy = []
    visited = set()
    visited.add(root_node)
    level = {root_node: 0}
    node_values = {root_node: 1}
    parents = {}
    children = {}

    while bfsQ:
        node = bfsQ.pop(0)
        bfsQ_copy.append(node)
        node_values[node] = 0

        # assigning node numbers. (sum of parents numbers)
        if node == root_node:
            node_values[node] = 1
        else:
            for parent in parents[node]:
                node_values[node] += node_values[parent]

        # collecting parent and children for each node
        node_children = network_component[node]
        for child_node in node_children:
            if child_node not in visited:
                bfsQ.append(child_node)
                visited.add(child_node)
                level[child_node] = level[node] + 1
                parents[child_node] = [node]
                if node in children:
                    children[node].append(child_node)
                else:
                    children[node] = [child_node]
            else:
                if level[child_node] == level[node] + 1:
                    parents[child_node].append(node)
                    if node in children:
                        children[node].append(child_node)
                    else:
                        children[node] = [child_node]

    # print("------------------------------")
    # print("Root Node: ", root_node)
    # print("Tree Level Dict: ", len(level), level)
    # print("Node values Dict: ", node_values)
    # print("Parent Dict: ", parents)
    # print("Child Dict: ", children)

    # calculating credits
    bfsQ_copy.reverse()
    reverse_bfsQ = bfsQ_copy
    edge_credit = []
    node_credit = {}

    for node in reverse_bfsQ:
        if node not in node_credit:
            node_credit[node] = 1.0
        if node in parents:
            node_parents = parents[node]
            if node in children:
                for node_parent in node_parents:
                    partial_credit = node_credit[node] / len(node_parents)
                    if node_parent not in node_credit:
                        node_credit[node_parent] = 1.0
                    node_credit[node_parent] += partial_credit
                    edge_credit.append((tuple(sorted([node, node_parent])), partial_credit))
            else:  # it is a leaf node in the bfs tree
                for node_parent in node_parents:
                    partial_credit = node_values[node_parent] / node_values[node]
                    if node_parent not in node_credit:
                        node_credit[node_parent] = 1.0
                    node_credit[node_parent] += partial_credit
                    edge_credit.append((tuple(sorted([node, node_parent])), partial_credit))

    return edge_credit


def get_modularity(original_network, network_components_rdds, m):
    '''
    :param orig_network: dict of original network
    :param network_components: list of rdds of component networks
    :param m: number of edges in original_network
    :return: modularity [-1, 1]
    '''
    network_components = []
    for rdd in network_components_rdds:
        network_components.append(rdd.collectAsMap())

    sum = 0
    for s in network_components:
        i_j_combo = list(combinations(list(s.keys()), 2))
        for i, j in i_j_combo:
            if j in original_network[i]:
                sum += (1 - (len(original_network[i]) * len(original_network[j])) / (2 * m))
            else:
                sum += (0 - (len(original_network[i]) * len(original_network[j])) / (2 * m))
    return sum / (2 * m)


# ----------------------------------------------------------------------------------------------------------------------

power_file_rdd = sc.textFile("power_input.txt").map(lambda x: (x.split(" ")[0], x.split(" ")[1]))
edges_rdd = power_file_rdd.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])]).distinct()

m = edges_rdd.count() / 2
print("Number of edges = ", m)
adj_list = edges_rdd.groupByKey().mapValues(lambda l: list(l))
original_network = adj_list.collectAsMap()

betweenness_centrality = adj_list.flatMap(
    lambda node: get_btw_cent_compnt(node[0], original_network)).groupByKey().mapValues(
    lambda l: sum(l) / 2).collect()

betweenness_centrality.sort(key=lambda x: x[0][0])
betweenness_centrality.sort(key=lambda x: x[1], reverse=True)

with open(str("task2a.txt"), "w") as file:
    for b in betweenness_centrality:
        file.write(str(b[0]) + ", " + str(b[1]) + "\n")
    file.close()


# COMMUNITIES

network_components = get_network_components([adj_list], None)
final_communities = []

max_modularity = -1.0
max_mod_network = tuple()
mod_decreased_count = 0
single_components = []

while True:
    edge_break_candidates = []
    # print("(While Loop) next Iter beginning network comps:", network_components)
    while_start = time()
    print("(While Loop) Num. of components = ", len(network_components))
    num_nodes = 0
    max_btw_cent = 0
    edge_to_break = None
    for component in network_components[:]:
        component_adj_list = component.collectAsMap()
        num_nodes += len(component_adj_list)

        if len(component_adj_list) == 1:
            single_components.append(component)
            print("Num of Singular components: ", len(single_components))
            network_components.remove(component)
            continue
        # print("(While Loop) Component Len = ", len(component_adj_list))

        btw_cent = component.flatMap(
            lambda node: get_btw_cent_compnt(node[0], component_adj_list)).groupByKey().mapValues(
            lambda l: sum(l) / 2).sortBy(lambda x: x[1]).collect()
        if btw_cent:
            if btw_cent[-1][1] > max_btw_cent:
                edge_to_break = btw_cent[-1][0]

    print("Sigma_nodes(components): ", num_nodes)

    new_network_components = get_network_components(network_components, edge_to_break)
    # print("(While Loop) New network components after breaking edge: ", new_network_components)

    new_modularity = get_modularity(original_network, new_network_components, m)

    # print("Max modularity= ", max_modularity, "|", "New modularity= ", new_modularity)

    if new_modularity > max_modularity:
        max_mod_network = (new_modularity, new_network_components+single_components)
        max_modularity = new_modularity
        mod_decreased_count = 0
    else:
        mod_decreased_count += 1

    network_components = new_network_components

    if len(single_components) == len(original_network): #mod_decreased_count > 25:
        break
    print("While loop one iter: ", time() - while_start, "s\n")


final_communities = max_mod_network[1]
final_comm = [sorted(community.collectAsMap().keys()) for community in final_communities]
final_comm.sort(key=lambda x: x[0])
final_comm.sort(key=lambda x: len(x))
print("Final Communities = ")
for community in final_comm:
    print(len(community), community)
with open(str("task2a_comm.txt"), "w") as file:
    for community_list in final_comm:
        if len(community_list) == 1:
            file.write(str("'" + community_list[0] + "'\n"))
        else:
            file.write(str("'" + community_list[0] + "'"))
            for num in range(1, len(community_list)):
                file.write(", ")
                file.write(str("'" + community_list[num] + "'"))
            file.write("\n")
    file.close()
print("Max Mod =", max_mod_network[0])
print("Time taken: ", time() - start_time, "s")
