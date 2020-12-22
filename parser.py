import header as h


def loadQuery(filename):
    '''
    This function creates all the data objects requires for the query tree
    and fills them.
    Let's not talk about the excessive use of global variables, ok?
    '''
    
    global V
    global E
    global P
    V, E, P = dict(), dict(), dict()

    global adj_out
    global adj_in
    adj_out, adj_in = dict(), dict()

    global vid
    global eid
    global pid
    vid, eid, pid = 0, 0, 0

    for line in open(filename).read().splitlines():
        parseLine(line)

    return h.graph(V, E, P, adj_out, adj_in)


def parseLine(line):  
    '''
    This function reads a query line and evaluates it to data objects. 
    It loads query tree vertices and edges according to the same datastructure as
    the loaded graph.
    '''
    
    end = line.find(']') + line[line.find(']'):len(line)].find(')') + 1
    if end != 0:
        triple = line[:end]

        edgeString = triple[triple.find(')') + 1:triple.rfind('(')]
        node1 = parseVertex(triple[:1 + triple.find(')')])
        node2 = parseVertex(triple[triple.rfind('('):])

        if '>' in edgeString:
            edge = parseEdge(edgeString, node1, node2)
        elif '<' in edgeString:
            edge = parseEdge(edgeString, node2, node1)
        else:  # undirected, add both
            edge = parseEdge(edgeString, node1, node2)
            edge_rev = parseEdge(edgeString, node2, node1)
            if edge_rev.id not in E:
                E.update({edge.id: edge_rev})
                adj_out.setdefault(edge_rev.src, set()).add((edge.id, edge_rev.trg))
                adj_in.setdefault(edge_rev.trg, set()).add((edge.id, edge_rev.src))

        if node1.id not in V:
            V.update({node1.id: node1})
        if node2.id not in V:
            V.update({node2.id: node2})
        if edge.id not in E:
            E.update({edge.id: edge})
            adj_out.setdefault(edge.src, set()).add((edge.id, edge.trg))
            adj_in.setdefault(edge.trg, set()).add((edge.id, edge.src))

        parseLine(line[line[:end].rfind('('):])


def parseVertex(vertexString):
    '''
    This function parses a vertex.
    '''
    
    vertexstr = vertexString[1:len(vertexString) - 1]  
    var = ''  
    label = '?'  # Default ? for unassigned query labels
    properties = dict()
    if len(vertexstr) != 0:  # If there is text in vertex
        leftbracket = vertexstr.find('{')
        if leftbracket != -1:  # Vertex has properties
            properties = eval(vertexstr[leftbracket:1 + vertexstr.find('}')])
            vertexstr = vertexstr[:leftbracket]
        doubledot = vertexstr.find(':')
        if doubledot != -1:  # Vertex has label
            label = vertexstr[doubledot + 1:]
            vertexstr = vertexstr[:doubledot]
        var = vertexstr
    if len(var) != 0:  # Vertex does have variable, i.e. UID. Requierd for determining query pattern
        return h.vertex(var, label, str(properties))
    else: 
        print('Vertex does not contain a variable, please change your query')


def parseEdge(edgeString, src, trg):
    '''
    This function parses an edge.
    '''
    edgestr = edgeString[edgeString.find('[') + 1:edgeString.find(']')]
    var = ''
    label = '?'  # Default ? for unassigned query labels in order for tree to recognize as unlabeled
    properties = dict()
    if len(edgestr) != 0:
        leftbracket = edgestr.find('{')
        if leftbracket != -1:  # Edge has properties
            properties = eval(edgestr[leftbracket:1 + edgestr.find('}')])
            edgestr = edgestr[:leftbracket]
        doubledot = edgestr.find(':')
        if doubledot != -1:  # Edge has label
            label = edgestr[doubledot + 1:]
            edgestr = edgestr[:doubledot]
        var = edgestr
    if len(var) != 0:  # Vertex does have variable, i.e. UID, for edges not required since vertices have UID
        return h.edge(var, label, src.id, trg.id, str(properties))
    else:  # Edge does not have variable, check if exact equal edge is parsed already, if yes return that one,
        # If no then return new one
        for edge in E.values():
            if edge.label == label and edge.properties == properties and edge.src == src and edge.trg == trg:
                return edge
        global eid
        e = h.edge(eid, label, src.id, trg.id, str(properties))
        eid += 1
        return e