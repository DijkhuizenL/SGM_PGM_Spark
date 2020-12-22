'''
Author: Luuk Dijkhuizen
Created: November 2019
'''

# -------------------------------------------------- PREP -------------------------------------------------- #

# Import python basic functions
import json
import time
import ast

# Initiate a pyspark session using findspark
import findspark
findspark.init('/home/mcs001/20121482/spark-3.0.0-bin-hadoop3.2')

# Import used pyspark functions
from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import udf, lit, when, struct, array, collect_set, explode, concat, concat_ws, coalesce
from pyspark.sql.types import StructType, StructField, IntegerType, BooleanType, StringType, ArrayType, LongType

# Import own python code created for this program
import parser as par
import querytrees as q
import header as h
import loader as l


def create_children_var(edges):
    '''
    Function required for generating a broadcast variable.
    The input of this function is the edges of the tree
    The output of this function is a dictionary with a vertex id as key and a list of vertex keys as value.
    The vertex id's in the dictionary's value are children of the key.
    '''
    children = dict()

    for e in edges.values():
        if e['src'] in children.keys():
            children[e['src']].append(e['dst'])
        else:
            children.update({e['src']: [e['dst']]})
    
    return children
    
def dropsolete_values(vertices, edges):

    Vt = dict()
    Et = dict() 

    for vertex in vertices.values():
        Vt.update({vertex.id:{'id': vertex.id,'label': vertex.label, 'properties': vertex.properties,
        'elist_id': vertex.elist.id, 'elist_direction':vertex.elist.direction, 'elist_parent':vertex.elist.parent}})

    for edge in edges.values():
        Et.update({edge.id:{'id':edge.id, 'label':edge.label, 'properties':edge.properties, 
        'src':edge.src, 'dst':edge.trg}})

    return Vt, Et

# -------------------------------------------------- PREP -------------------------------------------------- #
# ------------------------------------------------- PREGEL ------------------------------------------------- #
    
def outmsg_first_superstep(vid, label, properties, query_vertices, query_edges, query_leaves):

    outMsg = []
    
    for leaf in query_leaves:
        if (label == leaf['label'] or leaf['label'] == '?') and \
        (all(item in eval(properties).items() for item in eval(leaf['properties']).items())):
            outMsg.append((leaf['id'], [(leaf['id'], vid)]))

    if not outMsg:
        return None
    return outMsg


def udf_outmsg_first_superstep(query_vertices, query_edges, query_leaves):

    schema = ArrayType(StructType([
                StructField('tid_v', LongType(), True),
                StructField('msg', ArrayType(StructType([
                    StructField('tid_v', LongType(), True),
                    StructField('id_v', LongType(), True),
                    ])), True)
                ]), True)

    return udf(lambda vid, label, properties: outmsg_first_superstep(vid, label, properties, 
        query_vertices, query_edges, query_leaves), schema)


def outmsg_not_first_superstep(vid, label, properties, Mp, Res, messages, query_vertices, tree_children):

    mp_eval = eval(Mp) if Mp else Mp 
    res_eval = eval(Res) if Res else Res
    
    outMsg = set()
    matchV = set()

    for m in messages: 
        parent = query_vertices[query_vertices[m[0]]['elist_parent']]
        if (parent['label'] == '?' or parent['label'] == label) and \
        all(item in eval(properties).items() for item in eval(parent['properties']).items()):
            matchV.add(parent['id'])

            if not mp_eval:  # If Mp is empty
                mp_eval = {m[0]: frozenset([(i[0], i[1]) for i in m[1]])}
            else:  # If Mp is not empty 
                if m[0] in mp_eval.keys():  
                    # When tid from message in Mp.keys(), add mappings to the value for that key
                    omega = set(mp_eval[m[0]])
                    omega |= set([(i[0], i[1]) for i in m[1]])
                    mp_eval[m[0]] = frozenset(omega)
                else:
                    # Create new mapping if key not existent
                    mp_eval.update({m[0]: frozenset([(i[0], i[1]) for i in m[1]])})    

    for u_acc in matchV:
        children = tree_children[u_acc]
        if all(elem in mp_eval.keys() for elem in children):
            newOmega = set()
            newOmega.add((u_acc, vid))
            for child in children:
                for mapping in mp_eval[child]:
                    newOmega.add((mapping))
                del mp_eval[child]
            
            if u_acc == 0:   
                if not res_eval:  # Res is empty
                    res_eval = [list(newOmega)]
                else:  # Res is not empty, mappings are added
                    res_eval = res_eval.append(list(newOmega))
            else:
                outMsg.add((u_acc, frozenset(newOmega)))
    
    outMsg = None if not outMsg else [[x[0], list(x[1])] for x in outMsg]
    mp_eval = None if not mp_eval else str(mp_eval)
    if not res_eval: res_eval = None
        
    return [outMsg, mp_eval, res_eval]


def udf_outmsg_not_first_superstep(query_vertices, tree_children):
    
    schema = StructType([
            StructField('outMsg', 
                ArrayType(StructType([
                            StructField('tid_v', LongType(), True),
                            StructField('msg', ArrayType(StructType([
                                StructField('tid_v', LongType(), True),
                                StructField('id_v', LongType(), True),
                                ])), True)
                            ]), True), True),
            StructField('Mp', StringType(), True),
            StructField('Res', StructType([
                StructField('results', ArrayType(StructType([
                    StructField('t_id', LongType(), True),
                    StructField('v_id', LongType(), True)
                ]), True), True)
            ]), True)
    ])

    return udf(lambda vid, label, properties, Mp, Res, messages: 
    outmsg_not_first_superstep(vid, label, properties, Mp, Res, messages, 
    query_vertices, tree_children), schema)


def send_messages(vid, label, properties, src, dst, messages, query_vertices, query_edges):
    
    message_receiver = []

    for msg in messages:
        tree_vertex = query_vertices[msg[0]]
        tree_edge = query_edges[tree_vertex['elist_id']]
        if (tree_edge['label'] == '?' or tree_edge['label'] == label) and \
            all(item in eval(properties).items() for item in eval(tree_edge['properties']).items()):    
            if tree_vertex['elist_direction'] == 'reverse' and vid == dst:
                message_receiver.append((src, msg))
            if tree_vertex['elist_direction'] == 'forward' and vid == src: 
                message_receiver.append((dst, msg))
    
    return None if not message_receiver else message_receiver


def udf_send_messages(query_vertices, query_edges):

    schema = ArrayType(StructType([
        StructField('receiver', LongType(), True),
        StructField('message', StructType([
                StructField('tid_v', LongType(), True),
                    StructField('msgs', ArrayType(StructType([
                        StructField('tid_v', LongType(), True),
                        StructField('id_v', LongType(), True)])))
                        ]),True)
        ]),True)

    return udf(lambda vid, label, properties, src, dst, messages: 
        send_messages(vid, label, properties, src, dst, messages, query_vertices, query_edges), 
        schema)


def pregel(sc, sqlc, data_v, data_e, query_v, query_e, leaves, height, tree_children):
    '''
    The main pregel-based function.
    The runtime, active vertices and messagecount is kept.
    The query is cached in its whole.
    '''
    scoreboard = []
    
    # Broadcast variables 
    broadcast_query_v = sc.broadcast(query_v)
    broadcast_query_e = sc.broadcast(query_e)
    broadcast_leaves = sc.broadcast([query_v[l_id] for l_id in leaves])
    broadcast_children = sc.broadcast(tree_children)
    
    # Cache the edges
    data_e.cache()
    
    for it in range(height):
        print('st:',it, 'started.')
        sttime = time.time()
        # Determine active vertices
        if not it:  # First iteration
            active_vertices = data_v
        else:  # Not the first iteration
            # Re-cache active vertices due to update Mp/Res in previous superstep
            active_vertices.unpersist()
            # Do note that these are active vertices per message, due to the explode of outMsg in a previous superstep
            active_vertices = data_v.join(send_msg, data_v.id == send_msg.receiver).drop('receiver')
            
        active_vertices.cache()
        if not it:  # First iteration
            
            outMsg = when(lit(True), udf_outmsg_first_superstep(broadcast_query_v.value, broadcast_query_e.value, broadcast_leaves.value)
            (active_vertices['id'], active_vertices['label'], active_vertices['properties']))

        else:  # Not the first iteration
           
            # Get outMsg, Mp and Res
            outMsg_Mp_Res = when(lit(True), udf_outmsg_not_first_superstep(broadcast_query_v.value, broadcast_children.value)
            (active_vertices['id'], active_vertices['label'], active_vertices['properties'], active_vertices['Mp'], active_vertices['Res'], active_vertices['messages']))

            outMsg = outMsg_Mp_Res.getField('outMsg')
            Mp = outMsg_Mp_Res.getField('Mp')
            Res = outMsg_Mp_Res.getField('Res')

            # update Mp / Res
            tmp = active_vertices.withColumn('Mp_n', Mp).withColumn('Res_n', Res).select('id', 'Mp_n', 'Res_n')
            data_v = data_v.join(tmp, ['id'], how = 'left') \
                .withColumn('Mp_data', when((tmp['Mp_n'].isNotNull()), tmp['Mp_n']).otherwise(data_v['Mp'])) \
                .withColumn('Res_data', when((tmp['Res_n'].isNotNull()), tmp['Res_n']).otherwise(data_v['Res'])) \
                .drop('Mp_n', 'Res_n', 'Mp', 'Res') \
                .withColumnRenamed('Mp_data', 'Mp') \
                .withColumnRenamed('Res_data', 'Res')
        
        # Send messages by evaluating the corresponding edges for active vertices outmessages 
        activeMessages = active_vertices.withColumn('outMsg', outMsg).filter(outMsg.isNotNull()).select('id', 'outMsg')
        edge_check = data_e.join(activeMessages, (activeMessages['id'] == data_e['src']) | (activeMessages['id'] == data_e['dst']))
        
        # Create column for receivers
        rec = when(lit(True), udf_send_messages(broadcast_query_v.value, broadcast_query_e.value)
        (edge_check['id'], edge_check['label'], edge_check['properties'], edge_check['src'], edge_check['dst'], edge_check['outMsg'])) 
        
        # Count the number of messages
        message_count = edge_check.withColumn('rec', rec).filter(rec.isNotNull()).count() \
            # .select(explode('rec').alias('abc')).select('abc.*')
        
        # Aggregate messages per receiver for next superstep
        send_msg = edge_check.withColumn('rec', rec).filter(rec.isNotNull()) \
            .select(explode('rec').alias('abc')).select('abc.*') \
            .groupBy('receiver').agg(collect_set('message').alias('messages'))
        scoreboard.append(f'Superstep {it} done. Messages sent: {message_count}')

    for s in scoreboard:
        print(s)
    # Returns Res for all the vertices after the last superstep, i.e. the matched subgraphs
    return data_v.select('Res').where(data_v.Res.isNotNull())

# ------------------------------------------------- PREGEL ------------------------------------------------- #
# -------------------------------------------------- MAIN -------------------------------------------------- #

def main(sc, sqlc):
    startparse = time.time()

    # Find graph and query
    graph_filename = '/home/mcs001/20121482/data/ldbc.json'
    query_filename = 'queries/query.txt'

    # Load Graph & Query
    # V, E, adj_in, adj_out = l.load_graphson(graph_filename)
    Q = par.loadQuery(query_filename)
    V, E, adj_in, adj_out = l.load_ldbc(graph_filename)
    

    tree_type = 'MinMsg' # 'MinMsg' or 'SP'
    if tree_type == 'MinMsg': 
        try:
            stat_vertex, stat_edge, stat_joins = q.collect_statistics(V, E, adj_in, adj_out)
            T = q.get_tree(tree_type, Q.vertices, Q.edges, Q.adj_in, Q.adj_out, stat_vertex, stat_edge, stat_joins)
        except:
            print('One or more query label/property pair(s) is not in data, no patterns to be found.')
            print('Assuming you have not made any typos.')
            return False
    else:
        T = q.get_tree(tree_type, Q.vertices, Q.edges, Q.adj_in, Q.adj_out, False, False, False)
    
    Vt, Et = dropsolete_values(T.vertices, T.edges)
    
    
    print('Treetype:', tree_type)
    print("Tree vertices:")
    for v in Vt.values():
        print(v)
    
    print("Tree edges:")
    for e in Et.values():
        print(e)

    # Create adjacenty list broadcast variable children 
    tree_children = create_children_var(Et)
    
    # Create DataFrames
    v = sqlc.createDataFrame(V.values(), ['id', 'label', 'properties']) \
        .withColumn('Mp', lit(None).cast(StringType())) \
        .withColumn('Res', lit(None))
    e = sqlc.createDataFrame(E.values(), ['eid', 'label', 'src', 'dst', 'properties'])
    
    del V, E, adj_in, adj_out
    endparse = time.time()
    print("Loading and parsing time:", endparse - startparse, "seconds.")
    
    # Run Pregel
    startPregel = time.time()
    results = pregel(sc, sqlc, v, e, Vt, Et, T.leaves, T.height, tree_children)
    pregel_runtime = time.time() - startPregel
    
    # results.show(truncate=False)
    print("Runningtime Pregel:", pregel_runtime, "seconds.")
    
    

if __name__ == "__main__":
    '''
    Contains spark context, sql context, calls main
    '''
    # create Spark- and SQL context
    startconnectiontime = time.time()

    sparkContext = SparkContext(master='local[*]', appName='yourAppname')
    sparkContext.setCheckpointDir('/home/mcs001/20121482/spark_checkpoints')
    sqlContext = SQLContext(sparkContext)

    print("Connection time Spark:", time.time() - startconnectiontime, "seconds.")

    # Call main function and execute Pregel
    main(sparkContext, sqlContext)

    # End SparkContext
    sparkContext.stop()
