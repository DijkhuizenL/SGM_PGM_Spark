import json
import header as h



def load_graphson(filename):
    '''
    This function loads a Tinkerpop Graphson v3.0 (.json) file to data objects
    '''


    line_reader = open(filename).read().splitlines()
    vertices = dict()
    edges = dict()
    adj_in = dict()
    adj_out = dict()

    for t in line_reader:
        vertex = json.loads(t)
        v_id = vertex['id']['@value']
        if v_id not in vertices:  
            v_label = vertex['label']
            if 'inE' in vertex:
                for e_label, edgeList in vertex['inE'].items():
                    for edge in edgeList:
                        e_id = edge['id']['@value']
                        if e_id not in edges:
                            e_trg = v_id
                            e_src = edge['outV']['@value'] 
                            adj_in[e_trg] = adj_in.get(e_trg, dict())
                            adj_in[e_trg].update({e_id : e_src})
                            adj_out[e_src] = adj_out.get(e_src, dict())
                            adj_out[e_src].update({e_id : e_trg})
                            if 'properties' in edge:
                                e_properties = dict()
                                for p_prop, value in edge['properties'].items():
                                    if type(value) is not str:
                                        p_value = value['@value']
                                    else:
                                        p_value = value
                                    e_properties[p_prop] = p_value
                            else:
                                e_properties = dict()
                            edges.update({e_id: (e_id, e_label, e_src, e_trg, str(e_properties))})                                    
            if 'properties' in vertex:
                v_properties = dict()
                for p_prop, prop_list in vertex['properties'].items():
                    for v_property in prop_list:
                        if type(v_property['value']) is not str:
                            p_value = v_property['value']['@value']
                        else:
                            p_value = v_property['value']
                        v_properties[p_prop] = p_value
            else:
                v_properties = dict()
            vertices.update({v_id:(v_id, v_label, str(v_properties))})

    return vertices, edges, adj_in, adj_out

def load_ldbc(filename):
    line_reader = open(filename).read().splitlines()
    
    V = dict()
    E = dict()
    adj_in = dict()
    adj_out = dict()
    
    for line in line_reader:
        l = json.loads(line)

        vertices = l['vertices']
        edges = l['edges']

        for v in vertices:
            vid = v['_id']
            vlabel = v['xlabel']
            del v['_id'], v['xlabel']
            vproperties = str(v)
            V.update({vid : (vid, vlabel, vproperties)})

        for e in edges:
            eid = e['_id']
            elabel = e['_label']
            esrc = e['_outV']
            etrg = e['_inV']
            del e['_id'], e['_label'], e['_outV'], e['_inV']
            eproperties = str(e)
            E.update({eid: (eid, elabel, esrc, etrg, eproperties)})
            adj_in[etrg] = adj_in.get(etrg, dict())
            adj_in[etrg].update({eid : esrc})
            adj_out[esrc] = adj_out.get(esrc, dict())
            adj_out[esrc].update({eid : etrg})

    return V, E, adj_in, adj_out

def tree_dataframe_ready(tree):
    '''
    Converts tree object to dataframe-fitting data, allowing to create a pyspark DataFrame.
    '''
    Vt, Et = list(), list()

    for vertex in tree.vertices.values():
        Vt.append((vertex.id, vertex.label, str(vertex.properties), str(vertex.direction), str(vertex.elist.id),
                  str(vertex.elist.direction), str(vertex.elist.parent)))

    for edge in tree.edges.values():
        Et.append((edge.id, edge.label, edge.src, edge.trg, str(edge.properties), edge.direction))

    return Vt, Et, tree.root.id, tree.leaves, tree.height