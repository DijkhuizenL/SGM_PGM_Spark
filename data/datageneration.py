import random
import string
import csv
import time


def get_random_string(length):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))


def get_random_zipcode():
    return str(random.randint(1000, 9999)) + get_random_string(2).upper()


def create_address_vertex(i):
    return f'v{i} = g.addV(\'address\').property(id, {i}).property(\'zipcode\', \'{get_random_zipcode()}\')' \
           f'.property(\'housenumber\', {random.randint(1, 300)}).next()'


def create_company_vertex(i, sbi):
    perc_bankrupt = 40
    return f'v{i} = g.addV(\'company\').property(id, {i}).property(\'SBI\', \'{random.choice(sbi)}\'' \
           f').property(\'companyName\', \'{get_random_string(random.randint(4, 10))}\')' \
           f'.property(\'foundedYear\', \'{random.randint(1970, 2020)}\')' \
           f'.property(\'bankrupt\', {1 if random.randint(1, 100) <= perc_bankrupt else 0})' \
           f'.property(\'value\',{random.randint(100000, 5000000)})' \
           f'.property(\'nrOfEmployees\',{random.randint(1, 50)}).next()'


def create_person_vertex(i, nat):
    return f'v{i} = g.addV(\'person\').property(id, {i}).property(\'name\', ' \
           f'\'{get_random_string(1).upper()}{get_random_string(random.randint(3, 7))} {get_random_string(1).upper()}{get_random_string(random.randint(5, 11))}\')' \
           f'.property(\'age\', {random.randint(18, 75)})' \
           f'.property(\'nationality\',\'{random.choice(nat)}\').next()'


def create_crime_vertex(i, cri):
    return f'v{i} = g.addV(\'crime\').property(id, {i}).property(\'type\', \'{random.choice(cri)}\')' \
           f'.property(\'dateDay\', {random.randint(1, 30)}).property(\'dateMonth\', {random.randint(1, 12)})' \
           f'.property(\'dateYear\', {random.randint(1970, 2020)}).next()'


def create_building_vertex(i, func):
    return f'v{i} = g.addV(\'building\').property(id, {i}).property(\'function\', ' \
           f'\'{random.choice(func)}\').property(\'value\', {random.randint(50000, 2000000)})' \
           f'.property(\'nrPreviousOwners\', {random.randint(1, 5)}).next()'


def create_neighbourhood_vertex(i, func, dist, mun):
    return f'v{i} = g.addV(\'neighbourhood\').property(id, {i}).property(\'name\',' \
           f'\'{get_random_string(random.randint(5, 10))}\')' \
           f'.property(\'nrInhabitants\',{random.randint(500, 10000)})' \
           f'.property(\'mainFunction\', \'{random.choice(func)}\')' \
           f'.property(\'avgBuildingValue\',{random.randint(50000, 900000)})' \
           f'.property(\'district\', \'{dist[random.randint(0, len(dist) - 1)]}\')' \
           f'.property(\'municipality\', \'{random.choice(mun)}\').next()'


def create_committedAt_edge(i, vid_func, loc_cat):
    src = random.choice(vid_func['crime'])
    trg = random.choice(vid_func['address'])

    return f'g.addE(\'committedAt\').from(v{src}).to(v{trg}).property(id,{i})' \
           f'.property(\'extraInfo\', \'{get_random_string(random.randint(10, 20))}\')' \
           f'.property(\'locationCategory\', \'{random.choice(loc_cat)}\')'


def create_committedBy_edge(i, vid_func):
    src = random.choice(vid_func['crime'])
    trg = random.choice(vid_func['person'])
    perc_firsttimeoffender = 50
    punishments = ['Jail', 'Fine', 'Community Service']
    return f'g.addE(\'committedBy\').from(v{src}).to(v{trg}).property(id,{i})' \
           f'.property(\'firstCrime\', {1 if random.randint(1, 100) <= perc_firsttimeoffender else 0})' \
           f'.property(\'motivation\', \'{get_random_string(random.randint(6, 12))}\')' \
           f'.property(\'punishment\', \'{random.choice(punishments)}\')'


def create_livesAt_edge(i, vid_func):
    src = random.choice(vid_func['person'])
    trg = random.choice(vid_func['address'])
    return f'g.addE(\'livesAt\').from(v{src}).to(v{trg}).property(id,{i})' \
           f'.property(\'sinceDay\', {random.randint(1, 30)}).property(\'sinceMonth\', {random.randint(1, 12)})' \
           f'.property(\'sinceYear\', {random.randint(1970, 2020)})'


def create_related_edge(i, vid_func, rel_type):
    src = random.choice(vid_func['person'])
    trg = random.choice(vid_func['person'])

    return f'g.addE(\'related\').from(v{src}).to(v{trg}).property(id,{i})' \
           f'.property(\'typeOfRelation\', \'{random.choice(rel_type)}\')'


def create_reported_edge(i, vid_func):
    src = random.choice(vid_func['person'])
    trg = random.choice(vid_func['crime'])
    return f'g.addE(\'reported\').from(v{src}).to(v{trg}).property(id,{i})' \
           f'.property(\'dateDay\', {random.randint(1, 30)}).property(\'dateMonth\', {random.randint(1, 12)})' \
           f'.property(\'dateYear\', {random.randint(1970, 2020)})' \
           f'.property(\'testimony\', \'{get_random_string(random.randint(6, 12))}\')'


def create_registeredAt_edge(i, vid_func):
    src = random.choice(vid_func['building'] + vid_func['company'])
    trg = random.choice(vid_func['address'])
    return f'g.addE(\'registeredAt\').from(v{src}).to(v{trg}).property(id,{i})' \
           f'.property(\'sinceDay\', {random.randint(1, 30)}).property(\'sinceMonth\', {random.randint(1, 12)})' \
           f'.property(\'sinceYear\', {random.randint(1970, 2020)})' \
           f'.property(\'numberOfOwners\', {random.randint(1, 5)})'


def create_victimOf_edge(i, vid_func):
    src = random.choice(vid_func['crime'])
    trg = random.choice(vid_func['person'] + vid_func['company'])
    return f'g.addE(\'victimOf\').from(v{src}).to(v{trg}).property(id,{i})' \
           f'.property(\'reason\', \'{get_random_string(random.randint(6, 12))}\')'


def create_owns_edge(i, vid_func):
    src = random.choice(vid_func['person'] + vid_func['company'])
    trg = random.choice(vid_func['building'] + vid_func['company'])
    return f'g.addE(\'owns\').from(v{src}).to(v{trg}).property(id,{i})' \
           f'.property(\'sinceDay\', {random.randint(1, 30)}).property(\'sinceMonth\', {random.randint(1, 12)})' \
           f'.property(\'sinceYear\', {random.randint(1970, 2020)})'


def create_pays_edge(i, vid_func):
    src = random.choice(vid_func['person'])
    trg = random.choice(vid_func['person'])
    return f'g.addE(\'pays\').from(v{src}).to(v{trg}).property(id,{i})' \
           f'.property(\'amount\', {random.randint(1000, 100000)})'

def create_in_edge(i, vid_func):
    src = random.choice(vid_func['address'])
    trg = random.choice(vid_func['neighbourhood'])
    return f'g.addE(\'in\').from(v{src}).to(v{trg}).property(id,{i})' \

def create_data(graph_name, nr_vertices, nr_edges):
    data = ['graph = TinkerGraph.open()', 'g = graph.traversal()']

    func = ['Living', 'Business', 'Meeting', 'Healthcare', 'Office', 'Industrial', 'Tourism', 'Education',
            'Recreation', 'Misc']
    sbi = get_sbi_codes('/home/mcs001/20121482/luuql_repo/luuql/data/SBIcodes.csv')
    cri = ['Laundering', 'Break-in', 'Theft', 'Drug waste', 'Murder', 'Robbery', 'Fraud']
    neigh = ['Residential', 'Commercial', 'Industrial']
    nat = ['Antilles', 'Canadian', 'Dutch', 'French', 'German', 'Indonesian', 'Moroccan', 'Mexican', 'Polish',
           'Romanian', 'Surinamese', 'Turkish']
    districts = get_districts(150)
    municipalities = get_districts(50)
    # districs moet eigenlijk onder municipalities vallen, dat districts onderdeel zijn van municipality
    vertex_id_functions = {'address': [], 'building': [], 'company': [], 'crime': [], 'neighbourhood': [], 'person': []}

    distribution_vertices = [10, 8, 7, 10, 5, 15]  # address:building:company:crime:neighbourhood:person:
    for i in range(nr_vertices):
        roll = random.randint(1, sum(distribution_vertices))
        if sum(distribution_vertices[:0]) < roll <= sum(distribution_vertices[:1]):
            data.append(create_address_vertex(i))
            vertex_id_functions['address'].append(i)
        elif sum(distribution_vertices[:1]) < roll <= sum(distribution_vertices[:2]):
            data.append(create_building_vertex(i, func))
            vertex_id_functions['building'].append(i)
        elif sum(distribution_vertices[:2]) < roll <= sum(distribution_vertices[:3]):
            data.append(create_company_vertex(i, sbi))
            vertex_id_functions['company'].append(i)
        elif sum(distribution_vertices[:3]) < roll <= sum(distribution_vertices[:4]):
            data.append(create_crime_vertex(i, cri))
            vertex_id_functions['crime'].append(i)
        elif sum(distribution_vertices[:4]) < roll <= sum(distribution_vertices[:5]):
            data.append(create_neighbourhood_vertex(i, neigh, districts, municipalities))
            vertex_id_functions['neighbourhood'].append(i)
        elif sum(distribution_vertices[:5]) < roll <= sum(distribution_vertices[:6]):
            data.append(create_person_vertex(i, nat))
            vertex_id_functions['person'].append(i)

    loc_cat = ['Living', 'Catering/Bars', 'Community center', 'SMEs', 'Misc']
    relation_type = ['Sibling', 'Cousin', 'Uncle/Aunt', 'Parent', 'Grandparent', 'Friend']

    edge_distribution = [6, 10, 2, 5, 3, 10, 10,
                         6, 3, 5]  # committedAt:committedBy:livesAt:owns:registeredAt:related:reported:victimOf:pays:in
    for j in range(nr_edges):
        roll = random.randint(1, sum(edge_distribution))
        if sum(edge_distribution[:0]) < roll <= sum(edge_distribution[:1]):
            data.append(create_committedAt_edge(j, vertex_id_functions, loc_cat))
        elif sum(edge_distribution[:1]) < roll <= sum(edge_distribution[:2]):
            data.append(create_committedBy_edge(j, vertex_id_functions))
        elif sum(edge_distribution[:2]) < roll <= sum(edge_distribution[:3]):
            data.append(create_livesAt_edge(j, vertex_id_functions))
        elif sum(edge_distribution[:3]) < roll <= sum(edge_distribution[:4]):
            data.append(create_owns_edge(j, vertex_id_functions))
        elif sum(edge_distribution[:4]) < roll <= sum(edge_distribution[:5]):
            data.append(create_registeredAt_edge(j, vertex_id_functions))
        elif sum(edge_distribution[:5]) < roll <= sum(edge_distribution[:6]):
            data.append(create_related_edge(j, vertex_id_functions, relation_type))
        elif sum(edge_distribution[:6]) < roll <= sum(edge_distribution[:7]):
            data.append(create_reported_edge(j, vertex_id_functions))
        elif sum(edge_distribution[:7]) < roll <= sum(edge_distribution[:8]):
            data.append(create_victimOf_edge(j, vertex_id_functions))
        elif sum(edge_distribution[:8]) < roll <= sum(edge_distribution[:9]):
            data.append(create_pays_edge(j, vertex_id_functions))
        elif sum(edge_distribution[:9]) < roll <= sum(edge_distribution[:10]):
            data.append(create_in_edge(j, vertex_id_functions))

    data.append(f'graph.io(graphson()).writeGraph("{graph_name}.json")')
    return data


def get_sbi_codes(filename):
    SBIcodes = []
    with open(filename) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            SBIcodes.append(int(row[0]))
    return SBIcodes


def get_districts(amount):
    dist = [0] * amount
    for i in range(len(dist) - 1):
        dist[i] = get_random_string(1).upper() + get_random_string(random.randint(5, 12))
    return dist


def write_to_file(filename, data):
    outF = open(f"/home/mcs001/20121482/data/{filename}.txt", "w")
    for line in data:
        # write line to output file
        outF.write(line)
        outF.write("\n")
    outF.close()


if __name__ == '__main__':
    start = time.time()

    graphname = 'shintodata'
    filename = 'shintodata'

    nr_vertices = 100000
    nr_edges = 1000000

    data = create_data(graphname, nr_vertices, nr_edges)

    write_to_file(filename, data)

    print('Elapsed time:', time.time() - start)