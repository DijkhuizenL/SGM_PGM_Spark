'''
Author: Luuk Dijkhuizen
Created: Oktober 2019
Description: Contains the vertex and edge class and all the computational functions for Pregel
'''
import ast

dir_forward = 'forward'
dir_reverse = 'reverse'

class graph(object):
    '''
    The main graph object
    '''
    def __init__(self, vertices, edges, properties, adj_out, adj_in):
        self.vertices = vertices
        self.edges = edges
        self.properties = properties
        self.adj_out = adj_out
        self.adj_in = adj_in

class tree(object):
    '''
    The tree object
    '''
    
    def __init__(self, vertices, edges):
        self.vertices = vertices
        self.edges = edges
        self.root = self.vertices[0] # root is the first added vertex i.e. vid == 0
        self.height = self.getHeight()
        self.leaves = self.getLeaves()
        
    def getHeight(self):
        '''
        Gets the height of the tree using BFS
        O(V+E)
        '''

        E = self.edges.copy()
        nextlevel = [self.root.id]
        height = 0  # needs to start at 0 since range(3) loops over 0,1,2 and iteration nextlevel is empty after the last iter, if all the children have been found

        while nextlevel:
            thislevel = nextlevel
            nextlevel = []
            height += 1
            for v in thislevel: 
                for key, edge in E.copy().items():
                    if v == edge.src:
                        nextlevel.append(edge.trg)
                        del E[key] # key is unique per edge, only 1 value per key hence delete key
                    elif v == edge.trg: # dir is reverse
                        nextlevel.append(edge.src)
                        del E[key] 
        return height

    def getLeaves(self): 
        '''
        Gets all the leaves in the SP-tree using BFS
        O(V+E)
        '''
        E = self.edges.copy()
        leaves = []
        nextlevel = [self.root.id]
        
        while nextlevel:
            v = nextlevel[0]
            nextlevel.pop(0)
            size = len(nextlevel)
            for key, edge in E.copy().items():
                if edge.src == v:
                    nextlevel.append(edge.trg)
                    del E[key]
                    
            if len(nextlevel) == size:
                leaves.append(v)
        
        return leaves

class vertex(object):
    '''
    A vertex in graph G
    '''

    def __init__(self, id, label, properties, direction = str(None), elist = str(None)):
        self.id = id
        self.label = label
        self.properties = properties
        self.direction = direction
        self.elist = elist
        self.val = val(self.label, dict(),set())
      
class edge(object):
    '''
    An edge in graph G
    '''

    def __init__(self, uid, label, src, trg, properties, direction = str(None)):
        self.id = uid
        self.label = label
        self.src = src
        self.trg = trg
        self.properties = properties
        self.direction = direction

class property(object):

    def __init__(self, uid, propertytype, value):
        self.id = uid
        self.type = propertytype
        self.value = value

class elist():
    def __init__(self, id, direction, parent):
        self.id = id #edge id
        self.direction = direction #opposite direction 
        self.parent = parent #elist None if root, else ID

class val():
    def __init__(self, label, mp, res):
        self.label = label
        self.mp = mp
        self.res = res

def child(vid, graph) -> set:

    '''
    Returns the set of children objects of a vertex from tree
    A child is defined as nodes connected by an edge a level deeper in the tree
    O(E), since directed tree best option
    '''
    children = []
    for e in graph.edges.values():
        if e.src == vid:
            children.append(e.trg)                            
    return children

def inEdges(vertex, E) -> list:
    '''
    Returns a list of the incoming edges (v)<--- of a vertex 
    '''
    inEdges = []

    for e in E.values():
        if e.trg == vertex.id:
            inEdges.append(e)

    return inEdges

def outEdges(vertex, E) -> list:
    '''
    Returns a list of the outgoing edge id's (v)---> of a vertex 
    '''
    outEdges = []

    for e in E.values():
        if e.src == vertex.id:
            outEdges.append(e)

    return outEdges

def strToDict(string):
    return ast.literal_eval(string)

def dictToStr(dictionary):
    return str(dictionary)

class statistics(object):

    def __init__(self):
        self.labels = dict()
        self.properties = dict()
        self.lab_prop = dict()

    def add_label(self, label):
        self.labels[label] = self.labels.get(label, 0) + 1
        self.lab_prop[label] = self.lab_prop.get(label, dict())

    def add_property(self, label, prop):
        self.properties[prop] = self.properties.get(prop, 0) + 1
        self.lab_prop[label][prop] = self.lab_prop[label].get(prop, 0) + 1
        # label is always added before properties, hence this does not trigger key error


class joins(object):
    
    def __init__(self):
        self.vertex_in = dict()
        self.vertex_out = dict()
    
    def add_inedge(self, v_label, e_label):
        self.vertex_in[v_label] = self.vertex_in.get(v_label, dict())
        self.vertex_in[v_label][e_label] = self.vertex_in[v_label].get(e_label, 0) + 1
        #self.vertex_in[e_label][v_label] = self.vertex_in.get(e_label, dict()).get(v_label, 0) + 1

    def add_outedge(self, v_label, e_label):
        # self.vertex_out[e_label][v_label] = self.vertex_in.get(e_label, dict()).get(v_label, 0) + 1
        self.vertex_out[v_label] = self.vertex_out.get(v_label, dict())
        self.vertex_out[v_label][e_label] = self.vertex_out[v_label].get(e_label, 0) + 1