from __future__ import division
from tabulate import tabulate
import os
import argparse
import time
import sys
import random
from rdflib import URIRef
from rdflib import BNode
from rdflib import Graph

import LSDisc_MinHash

#   define parser for input arguments
#   1 mandatory argument:
    #   input_file      -> dataset file
#   3 optional arguments:
    #   hash_function   ->  MinHash or Random Projection
    #   ground truth    ->  generate ground truth file
    #   incremental     ->  build index incrementaly
def init_parser():
    parser = argparse.ArgumentParser(prog="LSDisc")
    parser.add_argument("input_file", help="input dataset")
    parser.add_argument("-hf", "--hash_function", help="Define LSH Family. mh -> MinHash, rp -> Random Projection", nargs=1, choices=["mh", "rp"])
    parser.add_argument("-i", "--incremental", help="Build the Index incrementaly.",
                        action="store_true")
    parser.add_argument("-gt", "--ground_truth", help="Construct and output ground truth file.",
                        action="store_true")

    return parser



def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size




def print_args(args):
    #   read input arguments
    print("\n\tInput Arguments:")
    dataset = str(args.input_file)
    print("\t\tDataset: \t%s" %dataset)
    if args.hash_function == None or args.hash_function[0] == "mh":
        print("\t\tLSH Family: \tMinHash")
    else:
        print("\t\tLSH Family: \tRandom Projection")
    if args.ground_truth:
        #LSDisc_General.printGreen("Ground Truth Mode: ON")
        print("\t\tGround Truth: \tON")
    else:
        print("\t\tGround Truth: \tOFF")

    if args.incremental:
        #LSDisc_General.printGreen("Incrementality Mode: ON")
        print("\t\tIncrementality: ON")
    else:
        print("\t\tIncrementality: OFF")
    print "\n"



#	print in red colour
def printRed(message): 
	print("\033[91m{}\033[00m" .format(message))


#	print in green colour
def printGreen(message):
	print("\033[92m{}\033[00m" .format(message)) 


#	compute jaccard similarity between two lists
def jaccard_similarity(list1, list2):
    intersection = len(list(set(list1).intersection(list2)))
    union = (len(list1) + len(list2)) - intersection
    if intersection == 0:
    	return 0
    else:
    	return float(intersection) / union



def create_gold_standard(g, dataset):

    #   name and path for ground truth file
    filename = "output/" + str(dataset.split('.')[-2].split('/')[-1]) \
                + '/' + str(dataset.split('.')[-2].split('/')[-1]) + "_gt.cnl"

    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    f = open(filename, "w")

    
    type_property = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
    distinct_type_objects_query = "SELECT DISTINCT ?o WHERE { ?s " + type_property + " ?o .}"

    #   query to retrieve distinct objects with type property from graph
    unique_type_objects = g.query(distinct_type_objects_query)

    #unique_type_objects_2 = g.objects(predicate=URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'))

    #unique_type_objects_2 = set(unique_type_objects_2)

    b_node_counter = 0
    for type_object in unique_type_objects:
        #   results of g.query are of type ResultRow. Use x[0] to get URIRef
        type_object = type_object[0].encode('utf-8')

        subjects = set(g.subjects(predicate=URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), object=URIRef(type_object)))

        f.write(type_object + "\t")

        for id_instance, instance in enumerate(subjects):

            if type(instance) == BNode:
                instance = "BNode_" + str(b_node_counter)
                b_node_counter += 1
            
            if id_instance != 0: 
                f.write(' ')

            f.write(instance.encode('utf-8'))

        f.write('\n')

    f.close()

    print("Ground truth file: %s\n" %filename)




def get_type_of_instance(instance):
    types = list()

    for type in types_and_instances.keys():
        if instance in types_and_instances[type]:
            types.append(type)

    return types



#   load dataset in graph
def load_graph(dataset):
    
    g = Graph()

    #   get format of dataset, to be used in g.parse()
    data_format = str(dataset.split('.')[-1])

    start_time_load = time.time()

    try:
        if data_format == 'rdf':
            g.parse(dataset)
        elif data_format == 'nt' :
            g.parse(dataset, format='nt')
    except Exception as e:# xml.sax._exceptions.SAXParseException as e:
        #   some .rdf files produce this exception because they are not well formed as rdf.
        #   in this case, use .nt as format
        printRed("\tException: " + str(e))
        printRed("\tTrying to import graph with '.nt' format.\n")
        g.parse(dataset, format='nt')
    
    end_time_load = time.time()
    
    print("\tGraph loaded successfully in %0.03f seconds." %(end_time_load - start_time_load))
    print("\tGraph contains %d tiples.\n" %len(g))

    return g



#   get unique instances in dataset
def get_unique_instances(g):
    #   --------------- WAY 1 ----------------------------------------------------------------------
    #   query to retrieve distinct instances from graph
    #   much slower than g.subjects() 
    '''unique_instances = g.query("""SELECT DISTINCT ?s
                                    WHERE {
                                        ?s ?p ?o .
                                    }""")

    print("Graph contains %d unique instances." %len(list(unique_instances)))'''

    #   query to retrieve distinct instances WITH declared type from graph
    #   TOO SLOW !
    '''unique_instances_with_type = g.query(""" SELECT DISTINCT ?s
                                        WHERE {
                                            ?s ?p ?o 
                                                filter exists {?s rdf:type ?o}
                                        } """)'''


    #print("Graph contains %d unique instances with declared type." %len(list(unique_instances_with_type)))

    #print("Graph contains %d instances with no type declared." %(len(list(unique_instances)) - len(list(unique_instances_with_type))))
    #   ---------------------------------------------------------------------------------------------

    #   using g.subjects() is faster
    #   we can filter out instances with no type declared later during hashing 
    #   get all subjects with no pattern on predicate or object
    return set(g.subjects(predicate=None, object=None))



def get_unique_instances_with_type(g):

    all_instances = get_unique_instances(g)
    instances_with_type = set()

    for instance in all_instances:
        types = set(g.objects(subject=instance, predicate=URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type')))
        if len(types) != 0:
            instances_with_type.add(instance)

    return instances_with_type



#   Get outgoing properties for given instance, remove properties from RDFS/OWL vocabularies
#   and add extension '.Out'
def get_outgoing_properties(instance, g):
    extension = unicode('.Out')

    #   get outgoing properties of instance
    outgoing_predicate_list = g.predicates(subject=URIRef(instance), object=None)
    
    #   use set to remove duplicates
    outgoing_predicate_list = set(outgoing_predicate_list)

    #   properties from RDFS and OWL vocabularies, e.g. rdfs:label, owl:sameAs must be excluded from 
    #   the description of the instances  because they can be applied to anyinstance regardless of its type 
    outgoing_predicate_list -= {URIRef('http://www.w3.org/2002/07/owl#sameAs'), \
                                URIRef('http://www.w3.org/2000/01/rdf-schema#label'), \
                                URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), \
                                URIRef('http://www.w3.org/2000/01/rdf-schema#seeAlso'), \
                                URIRef('http://www.w3.org/2000/01/rdf-schema#comment'), \
                                URIRef('http://www.w3.org/2004/02/skos/core#subject') }

    #   For outgoinf properties, add ".Out" extension
    #   n3() method, checks URI and adds <,> 
    outgoing_predicate_list = map(lambda x: (x.n3() + extension).encode('utf-8'), outgoing_predicate_list)

    return outgoing_predicate_list




#   Get ingoing properties for given instance, remove properties from RDFS/OWL vocabularies
#   and add extension '.In'
def get_ingoing_properties(instance, g):
    extension = unicode('.In')

    #   get outgoing properties of instance
    ingoing_predicate_list = g.predicates(subject=None, object=instance)

    #use set to remove duplicates
    ingoing_predicate_list = set(ingoing_predicate_list)

    #   properties from RDFS and OWL vocabularies, e.g. rdfs:label, owl:sameAs must be excluded from 
    #   the description of the instances  because they can be applied to anyinstance regardless of its type 
    ingoing_predicate_list -= {URIRef('http://www.w3.org/2002/07/owl#sameAs'), \
                                URIRef('http://www.w3.org/2000/01/rdf-schema#label'), \
                                URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), \
                                URIRef('http://www.w3.org/2000/01/rdf-schema#seeAlso'), \
                                URIRef('http://www.w3.org/2000/01/rdf-schema#comment'), \
                                URIRef('http://www.w3.org/2004/02/skos/core#subject') }

    #   For ingoing properties, add ".In" extension
    #   n3() method, checks URI and adds <,> 
    ingoing_predicate_list = map(lambda x: (x.n3() + extension).encode('utf-8'), ingoing_predicate_list)

    return ingoing_predicate_list




#   output buckets to <dataset>.cnl file
#   creates a new folder named 'output' where the .cnl file is placed
#   the instances in each bucket are printed in a separate line and splitted with ' '
def output_buckets(output_dir, buckets):

    filename = output_dir + ".cnl"

    #   create folder and file
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    f = open(filename, "w")

    for id_bucket, bucket in enumerate(buckets):

        for id_pattern, pattern in enumerate(bucket):
            f.write(str(pattern))
            
            if id_pattern != len(bucket)-1:
                f.write(' ')

        if id_bucket != len(buckets)-1:
            f.write("\n")

    f.close()
    print("\tGenerated buckets are contained in file: '%s'\n" %filename)



def output_patterns_instances(output_dir, dict_patterns):

    filename = output_dir + "_patterns.cnl"

    #   create folder and file
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    f = open(filename, "w")

    for pattern_str in dict_patterns.keys():
        patterns = dict_patterns[pattern_str]
        for p in patterns:
            f.write(str(p.getPatternNo()) + "\t")
            instances_of_pattern = p.getSetOfInstances()
            for id_inst, instance in enumerate(instances_of_pattern):
                f.write(str(instance))

                if id_inst != len(instances_of_pattern)-1:
                    f.write(' ')
            f.write("\n")

    f.close()
    print("\tGenerated patters are contained in file: '%s'\n" %filename)





def output_test_set(dataset, test_set):

    filename = "output/" + str(dataset.split('.')[-2].split('/')[-1]) \
                + '/' + str(dataset.split('.')[-2].split('/')[-1]) + "_test_set.cnl"

    #   create folder and file
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    f = open(filename, "w")

    for id_item, item in enumerate(test_set):

        f.write(item.encode('utf8'))
            
        if id_item != len(test_set)-1:
            f.write('\n')

    f.close()




#   output type profiles to <dataset>_type_profiles.cnl file
#   creates a new folder named 'output' where the .cnl file is placed
#   each type profile is a seperate line
def output_type_profiles(dataset, list_type_profiles):

    filename = "output/" + str(dataset.split('.')[-2].split('/')[-1]) + "/" \
                + str(dataset.split('.')[-2].split('/')[-1]) + "_type_profiles.cnl"

    #   create folder and file
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    f = open(filename, "w")

    for pair in list_type_profiles:

        dict_type_profile = pair[0]
        counter = pair[1]

        for id_prop, prop in enumerate(dict_type_profile.keys()):
            f.write(str(prop.encode('utf-8')) + ": " + str(dict_type_profile[prop]))
            
            if id_prop != len(dict_type_profile.keys())-1:
                f.write(' | ')

        #if id_bucket != len(buckets)-1:
        f.write("\t" + str(counter) + "\n")

    f.close()
    print("\tGenerated type profiles are contained in file: '%s'\n" %filename)




def create_bucket_profile(bucket):

    type_profile = {}

    #   get the properties of each instance in bucket
    for instance in bucket:
        properties = LSDisc_MinHash.dict_instances_properties[instance]

        #   if a property is already contained in the dictionary, just update the counter
        #   else, add the property with counter = 1
        for prop in properties:
            if prop in type_profile.keys():
                type_profile[prop] += 1
            else:
                type_profile[prop] = 1

    #   compute the probability of each property by dividing each property count with the number of elements in the bucket
    type_profile = dict(map(lambda x: (x[0], x[1] / len(bucket)), type_profile.items()))

    return type_profile




def update_type_profile(instance, query_instance):

    type_profile, numer_of_instances = LSDisc_MinHash.dict_buckets_type_profiles[LSDisc_MinHash.dict_instances_buckets[instance]]

    properties = LSDisc_MinHash.dict_instances_properties[query_instance]

    for prop in properties:
        if prop in type_profile.keys():

            old_probability = type_profile[prop]
            new_probability = ((old_probability * numer_of_instances) + 1 ) / (numer_of_instances + 1)

            type_profile[prop] = new_probability

        else:
            type_profile[prop] = (1 / (numer_of_instances + 1))

    LSDisc_MinHash.dict_buckets_type_profiles[LSDisc_MinHash.dict_instances_buckets[instance]] = (type_profile, numer_of_instances+1)




def remove_duplicate_buckets(buckets):

    distinct_buckets = list()
    for bucket in buckets:
        if bucket not in distinct_buckets:
            distinct_buckets.append(bucket)
    print("\tNumgber of unique generated groups: : %d\n" %len(distinct_buckets))

    return distinct_buckets





def considerTypeInfo():
    prob = random.randint(0, 100) / 100

    #   if type_existance_probability == 0 and prob == 0, then the condition prob <= type_existance_probability would be true
    #   which is wrong. So in this case, return false
    if LSDisc_MinHash.type_existance_probability == 0 and prob == 0:
        return False
    else:
        if prob <= LSDisc_MinHash.type_existance_probability:
            return True
        else:
            return False



def calculate_average_score(result_file):
    f = open(result_file, "r")

    precision_sum = 0
    recall_sum = 0
    f1_sum = 0
    count = 0
    for line in f:
        if "-" in line:
            continue
        line = line[:-1]
        scores = line.split("\t")
        precision = scores[0]
        recall = scores[1]
        f1 = scores[2]
            
        precision_sum += float(precision)
        recall_sum += float(recall)    
        f1_sum += float(f1)
        count += 1

    average_precision = float(precision_sum/count)
    average_recall = float(recall_sum/count)
    average_f1 = float(f1_sum/count)

    return average_precision, average_recall, average_f1




'''def remove_duplicates_reduce_size(buckets, dict_instances_buckets):
    start_time = time.time()

    distinct_buckets = list()
    for bucket in buckets:
        if bucket not in distinct_buckets:
            distinct_buckets.append(bucket)

            for instance in dict_instances_buckets.keys():
                if buckets[dict_instances_buckets[instance]] == bucket:
                    dict_instances_buckets[instance] = distinct_buckets.index(bucket)

    end_time = time.time()
    print("\tReduced size of dict and removed duplicate buckets in %.04f seconds" %(end_time - start_time))
    return distinct_buckets, dict_instances_buckets'''