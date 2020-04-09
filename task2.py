from collections import defaultdict
from pyspark import SparkContext
from sys import argv
from time import time
from random import choice

sc = SparkContext()
sc.setLogLevel("ERROR")
start_time = time()


# ----------------------------------------------------------------------------------------------------------------------

def get_network_components(old_components, edge_to_break):
    old_network_components = old_components[:]

    new_components = []
    if edge_to_break is not None:
        # removing that edge
        for component in old_network_components:
            if edge_to_break[0] in component.keys():
                component[edge_to_break[0]].remove(edge_to_break[1])
                if len(component[edge_to_break[0]]) == 0:
                    del component[edge_to_break[0]]
                    new_components.append(dict([(edge_to_break[0], [])]))
            if edge_to_break[1] in component.keys():
                component[edge_to_break[1]].remove(edge_to_break[0])
                if len(component[edge_to_break[1]]) == 0:
                    del component[edge_to_break[1]]
                    new_components.append(dict([(edge_to_break[1], [])]))

    for component in old_network_components:
        # perform bfs and keep adding new components to new_components
        vertices = set(component.keys())
        visited = set()
        remaining_vertices = set(component.keys())
        while remaining_vertices:
            bfsQ = [choice(tuple(remaining_vertices))]
            new_component = dict([(bfsQ[0], component[bfsQ[0]])])
            while bfsQ:
                node = bfsQ.pop(0)
                visited.add(node)
                children = component[node]
                for child in children:
                    if child not in visited:
                        visited.add(child)
                        bfsQ.append(child)
                        if node not in new_component:
                            new_component[node] = children[:]
                        if child not in new_component:
                            new_component[child] = component[child][:]
            new_components.append(new_component)
            remaining_vertices = vertices.difference(visited)
    return new_components


def get_modularity(o_network, net_components, m):
    """
    :param o_network: dict of original network
    :param net_components: list of rdds of component networks
    :param m: number of edges in original_network
    :return: modularity [-1, 1]
    """
    sum_mod = 0.0
    for s in net_components:
        for i in s.keys():
            for j in s.keys():
                if j in o_network[i]:
                    sum_mod += (1 - (float(len(o_network[i]) * len(o_network[j])) / (2.0 * m)))
                else:
                    sum_mod += (0 - (float(len(o_network[i]) * len(o_network[j])) / (2.0 * m)))
    return sum_mod / (2 * m)


def task2_1(o_network):
    btw_cent_list = list()
    for node in o_network.keys():
        btw_cent_list.extend(get_betweenness_of_component(node, o_network))

    btw_cent_dict = defaultdict(int)
    for edge_credit in btw_cent_list:
        btw_cent_dict[edge_credit[0]] += (edge_credit[1] / 2.0)

    betweenness_centrality = list(btw_cent_dict.items())

    betweenness_centrality.sort(key=lambda x: (x[0][0], x[0][1]))
    betweenness_centrality.sort(key=lambda x: x[1], reverse=True)

    with open(str(argv[2]), "w") as file:
        for b in betweenness_centrality:
            file.write(str(b[0]) + ", " + str(b[1]) + "\n")
        file.close()


def find_communities(o_network, mod_calc_orig_net, m):
    max_modularity = -1.0
    max_mod_network = tuple()
    single_components = []
    network_components = get_network_components([o_network], None)

    while True:
        max_btw_cent = -1.0
        edge_to_break = None

        for component in network_components[:]:
            if len(component) == 1:
                single_components.append(component)
                network_components.remove(component)
                continue

            btw_cent_list = list()
            for node in component.keys():
                btw_cent_list.extend(get_betweenness_of_component(node, component))

            btw_cent_dict = defaultdict(int)
            for edge_credit in btw_cent_list:
                btw_cent_dict[edge_credit[0]] += (edge_credit[1] / 2.0)

            btw_cent = sorted(btw_cent_dict.items(), key=lambda x: (x[1], x[0][0], x[0][1]))

            if btw_cent and btw_cent[-1][1] > max_btw_cent:
                max_btw_cent = btw_cent[-1][1]
                edge_to_break = btw_cent[-1][0]

        new_network_components = get_network_components(network_components[:], edge_to_break)
        new_modularity = get_modularity(mod_calc_orig_net, new_network_components + single_components, m)

        if new_modularity > max_modularity:
            max_modularity = new_modularity
            max_mod_network = (new_modularity, [dict(x) for x in new_network_components + single_components])

        network_components = new_network_components

        if len(single_components) == len(mod_calc_orig_net):
            break

    final_communities = max_mod_network[1]
    final_comm = [sorted(community.keys()) for community in final_communities]
    final_comm.sort(key=lambda x: x[0])
    final_comm.sort(key=lambda x: len(x))

    # print("Final Communities = \n")
    # i = 1
    # for community in final_comm:
        # print(i, len(community), community)
        # i += 1

    with open(str(argv[3]), "w") as file:
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


def get_betweenness_of_component(root_node, network_component):
    bfsQ = [root_node]
    visited = set()
    visited.add(root_node)
    level = {root_node: 0}
    node_values = {root_node: 1.0}
    parents = {}
    children = {}

    node_index = 0
    while node_index <= len(bfsQ) - 1:
        node = bfsQ[node_index]
        node_values[node] = 0.0
        children[node] = set()

        # assigning node numbers. (sum of parents numbers)
        if node == root_node:
            node_values[node] = 1.0
        else:
            for parent in parents[node]:
                node_values[node] += node_values[parent]

        # collecting parent and children for each node
        node_children = network_component[node]
        for child_node in node_children:
            if child_node in visited:
                if level[child_node] == level[node] + 1:
                    parents[child_node].add(node)
                    children[node].add(child_node)
            else:
                bfsQ.append(child_node)
                visited.add(child_node)
                level[child_node] = level[node] + 1.0
                parents[child_node] = set()
                parents[child_node].add(node)
                children[node].add(child_node)
        node_index += 1

    # calculating credits
    edge_credit = list()
    node_credit = {}

    node_index = len(bfsQ) - 1
    while node_index >= 0:
        node = bfsQ[node_index]
        if node not in node_credit:
            node_credit[node] = 1.0
        if node in parents:
            node_parents = parents[node]
            if children[node]:
                sum_node_values = sum(node_values[p] for p in node_parents)
                for node_parent in node_parents:
                    partial_credit = float(node_credit[node]) * node_values[node_parent] / sum_node_values
                    if node_parent not in node_credit:
                        node_credit[node_parent] = 1.0
                    node_credit[node_parent] += partial_credit
                    edge_credit.append((tuple(sorted([node, node_parent])), partial_credit))
            else:  # it is a leaf node in the bfs tree
                for node_parent in node_parents:
                    partial_credit = float(node_values[node_parent]) / float(node_values[node])
                    if node_parent not in node_credit:
                        node_credit[node_parent] = 1.0
                    node_credit[node_parent] += partial_credit
                    edge_credit.append((tuple(sorted([node, node_parent])), partial_credit))
        node_index -= 1
    return edge_credit


# ----------------------------------------------------------------------------------------------------------------------

power_file_rdd = sc.textFile(str(argv[1])).map(lambda x: (x.split(" ")[0], x.split(" ")[1]))
edges_rdd = power_file_rdd.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])]).distinct()
m = edges_rdd.count() / 2  # number of UNdirected edges (because dividing by 2)
adj_list = edges_rdd.groupByKey().mapValues(lambda l: list(l)).persist()
original_network = adj_list.collectAsMap()
mod_calc_orig_net = adj_list.collectAsMap()

task2_1(original_network)
find_communities(original_network, mod_calc_orig_net, m)