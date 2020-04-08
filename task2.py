from pyspark import SparkContext
from sys import argv
from time import time
from random import choice

sc = SparkContext()
sc.setLogLevel("ERROR")
start_time = time()


# ----------------------------------------------------------------------------------------------------------------------

def get_network_components(old_components, edge_to_break):
    # nc_start_time = time()

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
        # print("(get_network_components) num of Components after removing edge: ", len(old_network_components))

    for component in old_network_components:
        # perform bfs and keep adding RDDs of new components to new_components
        vertices = set(component.keys())
        visited = set()
        # new_components =[]
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
    # print("get_network_components time: ", time()-nc_start_time,"s")
    return new_components


def get_btw_cent_compnt(root_node, network_component):
    # btw_start_time = time()
    bfsQ = [root_node]
    visited = set()
    visited.add(root_node)
    level = {root_node: 0}
    node_values = {root_node: 1.0}
    parents = {}
    children = {}

    precalc_time = time()

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

    # print("Precalc time:", (time()-precalc_time)*1000,"ms")

    # print("------------------------------")
    # print("Root Node: ", root_node)
    # print("Tree Level Dict: ", len(level), level)
    # print("Node values Dict: ", node_values)
    # print("Parent Dict: ", parents)
    # print("Child Dict: ", children)

    credit_time = time()

    # calculating credits
    # bfsQ_copy.reverse()
    # reverse_bfsQ = bfsQ_copy
    edge_credit = []
    node_credit = {}

    for node_index in range(len(bfsQ) - 1, -1, -1):
        node = bfsQ[node_index]
        if node not in node_credit:
            node_credit[node] = 1.0
        if node in parents:
            node_parents = parents[node]
            if children[node]:
                for node_parent in node_parents:
                    partial_credit = float(node_credit[node]) / len(node_parents)
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
    # print("get_btw_cnt: ", time()-btw_start_time,"s")
    # print("calc credit time = ", (time() - credit_time)*1000,"ms")
    return edge_credit


def get_modularity(o_network, net_components, m):
    '''
    :param orig_network: dict of original network
    :param network_components: list of rdds of component networks
    :param m: number of edges in original_network
    :return: modularity [-1, 1]
    '''
    # mod_start_time = time()
    sum = 0.0
    for s in net_components:
        for i in s.keys():
            for j in s.keys():
                if j in o_network[i]:
                    sum += (1 - (float(len(o_network[i]) * len(o_network[j])) / (2.0 * m)))
                else:
                    sum += (0 - (float(len(o_network[i]) * len(o_network[j])) / (2.0 * m)))
    # print("get_modularity time:", time()-mod_start_time,"s")
    return sum / (2 * m)


def task2_1(adj_list):
    betweenness_centrality = adj_list.flatMap(
        lambda node: get_btw_cent_compnt(node[0], original_network)).groupByKey().mapValues(
        lambda l: sum(l) / 2.0).collect()

    betweenness_centrality.sort(key=lambda x: x[0][0])
    betweenness_centrality.sort(key=lambda x: x[1], reverse=True)

    with open(str("task2a.txt"), "w") as file:
        for b in betweenness_centrality:
            file.write(str(b[0]) + ", " + str(b[1]) + "\n")
        file.close()


def find_communities(original_network, mod_calc_orig_net, m):
    max_modularity = -1.0
    max_mod_network = tuple()
    mod_decreased_count = 0
    single_components = []
    network_components = get_network_components([original_network], None)

    while True:
        max_btw_cent = -1.0
        edge_to_break = None

        for component in network_components[:]:
            if len(component) == 1:
                single_components.append(component)
                network_components.remove(component)
                continue

            btw_cent_time = time()
            btw_cent = sc.parallelize(component.items()).flatMap(
                lambda node: get_btw_cent_compnt(node[0], component)).groupByKey().mapValues(
                lambda l: sum(l) / 2.0).sortBy(lambda x: x[1]).persist().collect()

            if btw_cent and btw_cent[-1][1] > max_btw_cent:
                max_btw_cent = btw_cent[-1][1]
                edge_to_break = btw_cent[-1][0]
            print("btw_cent_time:", time() - btw_cent_time)

        nc_time = time()
        new_network_components = get_network_components(network_components[:], edge_to_break)
        print("get_net_comp time:", time() - nc_time, "s")

        mod_time = time()
        new_modularity = get_modularity(mod_calc_orig_net, new_network_components + single_components, m)
        print("get_mod time:", time() - mod_time, "s\n")

        if new_modularity > max_modularity:
            max_modularity = new_modularity
            max_mod_network = (new_modularity, [dict(x) for x in new_network_components + single_components])
            mod_decreased_count = 0
        else:
            mod_decreased_count += 1

        network_components = new_network_components

        if mod_decreased_count > 25:  # len(single_components) == len(mod_calc_orig_net): # mod_decreased_count > 25: ##
            break

    final_communities = max_mod_network[1]
    final_comm = [sorted(community.keys()) for community in final_communities]
    final_comm.sort(key=lambda x: x[0])
    final_comm.sort(key=lambda x: len(x))

    print("Final Communities = ")
    for community in final_comm:
        print(len(community), community)

    with open(str("task2_comm.txt"), "w") as file:
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


# ----------------------------------------------------------------------------------------------------------------------

power_file_rdd = sc.textFile("power_input.txt").map(lambda x: (x.split(" ")[0], x.split(" ")[1]))
edges_rdd = power_file_rdd.flatMap(lambda x: [(x[0], x[1]), (x[1], x[0])]).distinct()
m = edges_rdd.count() / 2  # number of UNdirected edges (because dividing by 2)
print("Edges- ", m)
adj_list = edges_rdd.groupByKey().mapValues(lambda l: list(l)).persist()
original_network = adj_list.collectAsMap()
mod_calc_orig_net = adj_list.collectAsMap()

task2_1(adj_list)
find_communities(original_network, mod_calc_orig_net, m)

# GO THROUGH WHOLE CODE, ESPECIALLY READ THROUGH THE FUNCTIONS, THE MAIN METHOD PART LOOKS FINE.
# Consider sorting the btw cent and then breaking the highest edge when there are multiple edges with the same btw.