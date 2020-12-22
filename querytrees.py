import ast
import operator
import math
import header as h


# ------------------------------------- MinMsg ------------------------------------- #

def collect_statistics(vertices, edges, adj_in, adj_out):
    '''
    This function collects statistics for the MinMsg Tree.
    '''
    stat_v = h.statistics()
    stat_e = h.statistics()
    stat_ve = h.joins()

    for vertex in vertices.values():
        stat_v.add_label(vertex[1])
        for prop in ast.literal_eval(vertex[2]):
            stat_v.add_property(vertex[1], prop)
    
    for edge in edges.values():
        stat_e.add_label(edge[1])
        for prop in ast.literal_eval(edge[4]):
            stat_e.add_property(edge[1], prop)
    
    for trg, value in adj_in.items():
        trg_label = vertices[trg][1]
        for edge, src in value.items():
            edge_label = edges[edge][1]
            src_label = vertices[src][1]
            stat_ve.add_inedge(trg_label, edge_label)
            stat_ve.add_outedge(src_label, edge_label)

    return stat_v, stat_e, stat_ve

'''def determine_root(vas, eas, adj_in, adj_out):
    scoreboard = {}
    for v, v_score in vas.items():
        edges = adj_in[v] if v in adj_in.keys() else set()
        out_edges = adj_out[v] if v in adj_out.keys() else set()
        edges |= out_edges
        for e in [x[0] for x in edges]: 
            v_score *= eas[e]
            scoreboard.update({v: v_score})
    # minimum score = least specific. Hence root
    return min(scoreboard.items(), key=operator.itemgetter(1))[0]'''


def get_vertex_match_prob(vertex, v_stat):
    prop = ast.literal_eval(vertex.properties)
    if prop:
        selectivity = 1    
        for p in prop:
            if vertex.label != '?': # GIVEN label, following values
                # P(prop AND label) = P(Prop | Label) * P(Label) 
                p_prop_given_label = v_stat.lab_prop[vertex.label][p] / sum(v_stat.lab_prop[vertex.label].values())
                p_label = v_stat.labels[vertex.label] / sum(v_stat.labels.values())
                selectivity *= p_prop_given_label * p_label
            else:
                selectivity *= v_stat.properties[p] / sum(v_stat.properties.values())  # deze makker beetje heftig, aangezien nu specifieker terwijl hogere matchchance met weinig values (e.g. maand)
        return selectivity
    elif vertex.label != '?':
        return v_stat.labels[vertex.label] / sum(v_stat.labels.values())
    return 1


def get_edge_match_given_class_prob(edge, e_stat): # bound predicate! 
    prop = ast.literal_eval(edge.properties)
    if prop: # P(prop | label), prop make it more selective
        selectivity = 1
        for p in prop:
            selectivity *= 1 / e_stat.lab_prop[edge.label][p]
        return selectivity
    return 1 #this is given the class for expected edge amount, hence no prop -> chance is 1


def get_classes(obj, stat): # attach corresponding stat for object type
    '''
    Determines the class of an object. Returns class if only one possibility, returns list of options if multiple
    '''
    prop = ast.literal_eval(obj.properties)
    if obj.label != '?':
        return [obj.label]
    elif prop:
        obj_class = []
        for label, value in stat.lab_prop.items(): # check if all keys are in class
            if all(keys in value for keys in prop):
                obj_class.append(label)
        return obj_class
    return stat.labels.keys()


def get_exp_edge_amnt(src, edge, trg, stat_v, stat_e, stat_joins, exp_matches_vert):
    class_src = get_classes(src, stat_v)
    class_edge = get_classes(edge, stat_e)
    class_trg = get_classes(trg, stat_v)

    edge_amnt_srcout, edge_amnt_trgin = 0, 0
    srcout_edge_combinations, trgin_edge_combinations = 0, 0
    temp_label = edge.label
    for c_edge in class_edge:
        edge.label = c_edge
        for c_src in class_src: # TODO: nu overzichtelijker, misschien in 1 formule maken ipv losse vars?
            if (c_src in stat_joins.vertex_out) and (c_edge in stat_joins.vertex_out[c_src]):
                v_matches = exp_matches_vert[src.id]
                exp_amount_of_edges = v_matches * stat_joins.vertex_out[c_src][c_edge] / stat_v.labels[c_src]
                prob_edgematch_class_ass = get_edge_match_given_class_prob(edge, stat_e)
                edge_amnt_srcout += exp_amount_of_edges * prob_edgematch_class_ass 
                srcout_edge_combinations += 1 
        for c_trg in class_trg:
            if (c_trg in stat_joins.vertex_in) and (c_edge in stat_joins.vertex_in[c_trg]):
                v_matches = exp_matches_vert[trg.id]
                exp_amount_of_edges = v_matches * stat_joins.vertex_in[c_trg][c_edge] / stat_v.labels[c_trg]
                prob_edgematch_class_ass = get_edge_match_given_class_prob(edge, stat_e)
                edge_amnt_trgin += exp_amount_of_edges * prob_edgematch_class_ass
                trgin_edge_combinations += 1
    
    edge.label = temp_label
    if srcout_edge_combinations == 0 or trgin_edge_combinations == 0:
        return False, False
    return edge_amnt_srcout/srcout_edge_combinations, edge_amnt_trgin/trgin_edge_combinations


def minmsg_tree_stats(Vq, Eq, stat_v, stat_e, stat_joins):

    exp_matches_vert = dict()
    v_tot = sum(stat_v.labels.values())
    
    for vertex in Vq.values(): 
        exp_matches_vert.update({vertex.id: get_vertex_match_prob(vertex, stat_v) * v_tot})

    expmsg_srcout = dict()
    expmsg_trgin = dict()

    for edge in Eq.values():
        e_msg_srcout, e_msg_trgin = get_exp_edge_amnt(Vq[edge.src], edge, Vq[edge.trg], stat_v, stat_e, stat_joins, exp_matches_vert)
        if not (e_msg_srcout or e_msg_trgin): 
            return False
        expmsg_srcout[edge.src] = expmsg_srcout.get(edge.src, dict())
        expmsg_srcout[edge.src].update({edge.id: e_msg_srcout})
        expmsg_trgin[edge.trg] = expmsg_trgin.get(edge.trg, dict())
        expmsg_trgin[edge.trg].update({edge.id: e_msg_trgin})

        #selectivity for edge required given class! 

    return expmsg_srcout, expmsg_trgin, exp_matches_vert

def est_msg_first_superstep(tree, vidMap, eidMap, srcout_stats, trgin_stats):
    expected_messages = 0
    next_superstep = set()
    sent_msg = dict()
    for leaf in tree.leaves:
        l = tree.vertices[leaf]
        next_superstep.add(l.elist.parent)
        if l.elist.direction == 'reverse':
            amount =  trgin_stats[vidMap[l.id]][eidMap[l.elist.id]]
            sent_msg.update({eidMap[l.elist.id]: amount})
            expected_messages += amount
        else:
            amount = srcout_stats[vidMap[l.id]][eidMap[l.elist.id]]
            sent_msg.update({eidMap[l.elist.id]: amount})
            expected_messages += srcout_stats[vidMap[l.id]][eidMap[l.elist.id]]

    return expected_messages, next_superstep, sent_msg


def parent_fraction(vertex, vidMap, eidMap, srcout_stats, trgin_stats, expected_vertex_match, sent_msg):

    fraction = 1
    vid = vidMap[vertex.id]
    evm = expected_vertex_match[vid]
    if vid in srcout_stats:
        srcout = srcout_stats[vid]
        for edgeid in srcout_stats[vid]:
            if edgeid != eidMap[vertex.elist.id] and (edgeid in sent_msg):
                edge_fraction = sent_msg[edgeid] / srcout[edgeid]
                if edge_fraction > 1: 
                    continue
                fraction *= edge_fraction  # Assume independence
    if vid in trgin_stats:
        trgin = trgin_stats[vid]
        for edgeid in trgin:
            if edgeid != eidMap[vertex.elist.id]  and (edgeid in sent_msg):
                edge_fraction = sent_msg[edgeid] / trgin[edgeid]
                if edge_fraction > 1: 
                    continue
                fraction *= edge_fraction  # Assume independence         
    return fraction 


def est_msg_superstep(vertices, tree, vidMap, eidMap, srcout_stats, trgin_stats, expected_vertex_match, sent_msg):
    expected_messages = 0
    next_superstep = set()
    updated_sent_msg = sent_msg
    for vertex in vertices:
        v = tree.vertices[vertex]
        if v.elist.direction == 'reverse':
            if v.elist.id != None: 
                next_superstep.add(v.elist.parent)
                frac = parent_fraction(v, vidMap, eidMap, srcout_stats, trgin_stats, expected_vertex_match, sent_msg)
                amount = trgin_stats[vidMap[v.id]][eidMap[v.elist.id]] * frac
                updated_sent_msg.update({eidMap[v.elist.id]:amount}) 
                expected_messages += amount

        else:
            if v.elist.id != None: 
                next_superstep.add(v.elist.parent)
                frac = parent_fraction(v, vidMap, eidMap, srcout_stats, trgin_stats, expected_vertex_match, sent_msg)
                amount = srcout_stats[vidMap[v.id]][eidMap[v.elist.id]] * frac 
                updated_sent_msg.update({eidMap[v.elist.id]:amount})
                expected_messages += amount      

    return expected_messages, next_superstep, updated_sent_msg

# ------------------------------------- MinMsg ------------------------------------- #
# ------------------------------------- SP-tree ------------------------------------- #

def eccentricity(node, Eq):
    '''
    Returns eccentricity of node by using BFS, which is the maximum of all the shortest paths to all other nodes in the graph
    Helper function of getRoot
    '''

    return max(shortestPathBFS(node, Eq).values())


def shortestPathBFS(node, Eq):
    '''
    Returns shortest path from a given node, recognized by ID
    Helper function of eccentricity
    O(V+E)
    '''
    nextlevel = {node.id: 1}
    return dict(singleShortestPathLength(Eq, nextlevel))


def singleShortestPathLength(Eq, firstlevel):
    """
    Yields (node, level) in a breadth first search
    Shortest Path BFS helper function
    O(V+E)
    """
    seen = {}  # level when seen in BFS
    level = 0  # the current level, root = 0
    nextlevel = firstlevel  # dict of nodes to check at next level

    while nextlevel:
        thislevel = nextlevel  # hop to next level
        nextlevel = {}  # and start a new list for next
        for v in thislevel:
            if v not in seen:
                seen[v] = level
                for e in Eq.values():  # required for both in and out edges due to undirected distance for center nodes
                    if e.src == v:
                        nextlevel.update({e.trg: level})
                    if e.trg == v:
                        nextlevel.update({e.src: level})
                yield (v, level)
        level += 1
    del seen


def get_SP_root(Vq, Eq):
    '''
    Returns the ID of the central node by finding a node with minimal eccentricity 
    This node will be the SP-tree root
    O(V)
    '''
    central_node = (None, float('inf'))

    for v in Vq.values():
        node = v
        ecc = eccentricity(node, Eq)
        if ecc < central_node[1]:
            central_node = (node.id, ecc)

    return central_node[0]

# ------------------------------------- SP-tree ------------------------------------- #
# --------------------------------- Tree Generation --------------------------------- #

def get_tree(type, Vq, Eq, adj_in, adj_out, stat_v, stat_e, stat_joins):

    if type == 'SP':
        u_r = get_SP_root(Vq, Eq)
        tree, vidMap, eidMap = generateTree_BFS(u_r, Vq, Eq, adj_in, adj_out, stat_v, stat_e, stat_joins)
        return tree
    elif type == 'MinMsg':
        src_out_stats, trg_in_stats, expected_vertex_match = minmsg_tree_stats(Vq, Eq, stat_v, stat_e, stat_joins)
        best_cardinality = 0
        for vertex in Vq:  # tree needs to be possible!
            tree, vidMap, eidMap = generateTree_BFS(vertex, Vq, Eq, adj_in, adj_out, stat_v, stat_e, stat_joins)
            tree_cardinality, next_superstep, sent_msg = est_msg_first_superstep(tree, vidMap, eidMap, src_out_stats, trg_in_stats)
            while next_superstep:
                leaf_cardinality, next_superstep, sent_msg = est_msg_superstep(next_superstep, tree, vidMap, eidMap, src_out_stats, trg_in_stats, expected_vertex_match, sent_msg)
                tree_cardinality += leaf_cardinality
            
            if best_cardinality == 0 or tree_cardinality < best_cardinality:
                best_cardinality = tree_cardinality
                best_tree = tree
                best_msg = sent_msg
        print('Expected messages:',best_msg)        
        return best_tree


def generateTree_BFS(u_r, Vq, Eq, adj_in, adj_out, stat_v, stat_e, stat_joins):
    '''
    Returns a tree with the edge and vertex objects based on the root node u_r
    Vt is a dict {key : set(object(s))}, if duplicate edges size(set) > 1s
    O(V+E)
    Tree
    vertices = { v.id : v_obj } 1 or more value(s) per key (uid)
    edges = { e.id : e_obj }
    root = vertex id from query graph, so loop to itself solves automatically
    '''

    Vt, Et, vidMap, eidMap = dict(), dict(), dict(), dict()  # Vt/Et is 1:1 key:value mapping
    eid, vid = 0, 0

    Vt.update({vid: h.vertex(vid, Vq[u_r].label, Vq[u_r].properties, direction=None, elist=h.elist(None, None, None))})
    vidMap.update({vid: u_r})
    
    K = [vid]  # K stores VID's, i.e. unique vertex id's for the SPtree, not the query graph
    vid += 1

    visited = []  # visited stores query graph ID's in order to properly append K
    Eq_temp = Eq.copy() #require copy to change size of Eq temp during iteration for edge check
    while K:
        u_1 = K[0]
        K.pop(0)
        visited.append(vidMap[u_1])  # stores vertex id of query, NOT TREE VID
        for key, edge in Eq_temp.copy().items():
            if edge.src == vidMap[u_1]:  # src = u_1, trg = u_2
                Vt.update({vid: h.vertex(vid, Vq[edge.trg].label, Vq[edge.trg].properties, direction=h.dir_forward,
                                         elist=h.elist(eid, h.dir_reverse, u_1))})
                vidMap.update({vid: edge.trg})
                Et.update({eid: h.edge(eid, edge.label, u_1, vid, edge.properties, h.dir_forward)})
                eidMap.update({eid: edge.id})
                del Eq_temp[key]
                if edge.trg not in visited:
                    K.append(vid)
                vid += 1
                eid += 1

            elif edge.trg == vidMap[u_1]:  # trg = u_1, src = u_2
                Vt.update({vid: h.vertex(vid, Vq[edge.src].label, Vq[edge.src].properties, direction=h.dir_reverse,
                                         elist=h.elist(eid, h.dir_forward, u_1))})
                vidMap.update({vid: edge.src})
                Et.update({eid: h.edge(eid, edge.label, u_1, vid, edge.properties, h.dir_reverse)})
                eidMap.update({eid: edge.id})
                del Eq_temp[key]
                if edge.src not in visited:
                    K.append(vid)
                vid += 1
                eid += 1

    return h.tree(Vt, Et), vidMap, eidMap