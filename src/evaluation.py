from __future__ import division
from tabulate import tabulate
from rdflib import URIRef
from collections import defaultdict
import sys
import argparse


import LSDisc_General 
import evaluation_incremental

#	------------------------------------------------------------------
#	--------------- Initialization of variables ----------------------

#	dict to store types and max groups
dict_types_max_groups = {}

#	number of instances in dataset
number_of_instances = 0

#	----------------------------------------------------------------

#	define parser for input arguments
#	2 mandatory arguments:
	# 	bucket_file 		-> bucket file from LSDisc
	#	ground_truth_file	-> file with ground truth
def init_parser():
	parser = argparse.ArgumentParser(prog="LSDisc Evaluation")
	parser.add_argument("bucket_file", help="File containing output of LSDisc")
	parser.add_argument("ground_truth_file", help="Ground Truth File")
	parser.add_argument("-i", "--incremental", help="Build the Index incrementaly.",
                        action="store_true")

	return parser


def print_args_eval(args):

	print("\n\tInput Arguments:")
	bucket_file = args.bucket_file
	print("\t\tBucket file: \t\t%s" %bucket_file)

	ground_truth_file = args.ground_truth_file
	print("\t\tGround Truth file: \t%s" %ground_truth_file)
	if args.incremental:
		#LSDisc_General.printGreen("Incrementality Mode: ON")
		print("\t\tIncrementality: \tON")
	else:
		print("\t\tIncrementality: \tOFF")
	print "\n"



#   calculate precision score
def precision(true_positives, false_positives):
    return float(true_positives / (true_positives + false_positives))



#   calculate recall score
def recall(true_positives, false_negatives):
    return float(true_positives / (true_positives + false_negatives))


#   calculate f1 score
def f1score(precision, recall):
    return 2* float((precision * recall) / (precision + recall))


def load_gold_standard(gold_standard_file):

	f = open(gold_standard_file, "r")

	instance_list = list()

	dict_types_instances = {}

	i = 0
	for line in f:

		#	split line using '\t'
		class_entry = line.split('\t')

		#	This case corresponds to ground truth files that have the form:
		#	class_name \t instnaces
		if len(class_entry) > 1:

			class_name = class_entry[0]

			instance_list = class_entry[1].split(' ')

			#	remove last element if it is '\n' or ''
			if instance_list[-1][-1] == '\n':
				instance_list[-1] = instance_list[-1][:-1]


		#	This case corresponds to ground truth files that have the form:
		#	instance_list
		#	and class_name is the line counter
		elif len(class_entry) == 1:
			instance_list = line.split(' ')

			#	remove last element if it is '\n' or ''
			if instance_list[-1][-1] == '\n':
				instance_list[-1] = instance_list[-1][:-1]

			class_name = str(i)

		#	remove possible duplicate class names using set()
		instance_list = set(instance_list)

		dict_types_instances[class_name] = instance_list

		i += 1

	global number_of_instances
	number_of_instances = len(set.union(*dict_types_instances.values()))

	print(tabulate([[str(len(dict_types_instances.keys())), number_of_instances]], ["Number of Types", "Number of Instances"]))
	print "\n"

	'''for type in dict_types_instances.keys():
		print("%s - %d" %(type, len(dict_types_instances[type])))'''

	return dict_types_instances



def load_buckets(bucket_file):

	f = open(bucket_file, "r")

	buckets = list()

	#	each line represents a bucket
	for line in f:

		#	split line by ' ' to get instances contained in bucket
		patterns = line.split(' ')

		#	remove '\n' if present
		if patterns[-1][-1] == '\n':
			patterns[-1] = patterns[-1][:-1]

		#print patterns[0]
		buckets.append(set(patterns))

	print("\tNumber of buckets: %d\n" %len(buckets))

	return buckets



def load_patterns_instances(pattern_instances_file, buckets):
	f = open(pattern_instances_file, "r")

	for line in f:

		entries = line.split('\t')
		pattern = entries[0]
		#print pattern

		instance_list = entries[1].split(' ')

		#	remove last element if it is '\n' or ''
		if instance_list[-1][-1] == '\n':
			instance_list[-1] = instance_list[-1][:-1]

		#	remove possible duplicate class names using set()
		instance_list = set(instance_list)

		for i in range(len(buckets)):
			if pattern in buckets[i]:
				buckets[i] = buckets[i].union(instance_list)
				buckets[i].remove(pattern)

	return buckets



#	store the max_containment_bucket for each type at dict_types_max_buckets
def find_max_bucket_for_type(buckets, dict_types_instances):

	dict_types_max_buckets = {}

	for type in dict_types_instances.keys():
		max_intersection = 0
		max_bucket_index = 0

		#	get instances of this type
		type_instances = dict_types_instances[type]

		#	get intersection with each bucket and store the one with max intersection
		for bucket in buckets:
			#	retrieve instances from hasnames
			#subject_list = map(lambda x: functions.dict_hashnames_instances[x], bucket)
			intersection = len(list(set(type_instances).intersection(bucket)))

			#	if the current bucket contains all the instances of the class
			#	and no other instance, then the bucket fully represents the class
			#	so considet it the max_cont_bucket and break
			if intersection == len(type_instances) and len(bucket) == len(type_instances):
				max_bucket_index = buckets.index(bucket)
				break


			#	if the current bucket contains all the instances of the class, consider it the max_containment_bucket
			#if intersection == len(type_instances):
			#	max_bucket_index = buckets.index(bucket)


			if intersection > max_intersection:
				max_intersection = intersection
				max_bucket_index = buckets.index(bucket)


			if intersection == max_intersection:
				if len(bucket) < len(buckets[max_bucket_index]):
					max_intersection = intersection
					max_bucket_index = buckets.index(bucket)

		dict_types_max_buckets[type] = max_bucket_index

	return dict_types_max_buckets



#	evaluation of buckets -> calculate scores for each class and total scores
def evaluation_buckets(buckets, dict_types_instances):
	
	#	find max_containment_bucket for each class
	dict_types_max_buckets = find_max_bucket_for_type(buckets, dict_types_instances)

	precision_sum = 0
	recall_sum = 0

	#	list to store scores for each class
	class_scores = list()

	for type in dict_types_instances.keys():

		true_positives = 0
		false_positives = 0
		false_negatives = 0

		#	get max_containment_bucket for this class
		max_bucket_index = dict_types_max_buckets[type]
		bucket = buckets[max_bucket_index]

		#	get instance of the class
		type_instances = dict_types_instances[type]

		#	retrieve instances from hasnames
		#subject_list = map(lambda x: functions.dict_hashnames_instances[x], bucket)
		#	get intersection of class instances with max_containment_bucket
		true_positives = len(list(set(type_instances).intersection(bucket)))
		if true_positives == 0:
			print dict_types_instances[type]
			print buckets[max_bucket_index]

		if true_positives == len(type_instances):
			false_negatives = 0
		else:
			false_negatives = len(type_instances) - true_positives

		if true_positives == len(bucket):
			false_positives = 0
		else:
			false_positives = len(bucket) - true_positives

		type_precision = precision(true_positives, false_positives)
		type_recall = recall(true_positives, false_negatives)
		type_f1_score = f1score(type_precision, type_recall)

		precision_sum += type_precision
		recall_sum += type_recall

		class_scores.append(type + "\t" + str(type_precision) + "\t" + str(type_recall) + "\t" + 
			str(type_f1_score) + "\n" )

	
	#	total accuracy scores
	total_precision = precision_sum / len(dict_types_instances.keys())
	total_recall = recall_sum / len(dict_types_instances.keys())
	f1_score = f1score(total_precision, total_recall)

	#	calculate capacity score
	capacity_score = evaluation_capacity_buckets(dict_types_instances)

	#	print results
	print(tabulate([	[str(total_precision), str(total_recall), str(f1_score), str(capacity_score)]],
										["Precision", "Recall", "f1_score", "capacity_score"]))
	print "\n"


	# write class scores to a file
	#write_class_scores_to_file(class_scores, "buckets")




#	Write precision, accuracy, f1score for easch class to a file for later use
#	parameter 'mode' can take two values: buckets and groups
def write_class_scores_to_file(class_scores, mode):
	file_name = "class_scores" + mode + ".txt"
	f = open(file_name, "w")
	for entry in class_scores:
		f.write(entry)
	f.close()



#	calculates capacity score on buckets
def evaluation_capacity_buckets(dict_types_instances):

	sum = 0
	for bucket in buckets:

		type_to_count = {}
		
		#	find how many types are contained in each bucket and store to dictionary type_to_count
		for type in dict_types_instances.keys():
			intersection = len(list(set(dict_types_instances[type]).intersection(bucket)))
			if intersection > 0:
				type_to_count[type] = intersection

		#	calculate score for each bucket
		bucket_score = 0	
		for type in type_to_count:
			bucket_score +=  float(type_to_count[type] / len(dict_types_instances[type]))

		if len(type_to_count.keys()) > 0:
			bucket_score = bucket_score / len(type_to_count.keys())
			sum += bucket_score

	#	calculate total capacity score for all buckets
	capacity_score = float(sum / len(buckets))

	return capacity_score



def print_containment_of_max_buckets_and_groups(buckets, dict_types_instances):

	dict_types_max_buckets = find_max_bucket_for_type(buckets, dict_types_instances)

	for type in dict_types_instances.keys():

		data = list()

		print("Containment of max_bucket and max_group for class: %s\n" %type)
		max_bucket = buckets[dict_types_max_buckets[type]]
		#max_group = functions.groups[dict_types_max_groups[type]]

		#subject_list_bucket = map(lambda x: dict_hashnames_instances[x], max_bucket)
		#subject_list_group = map(lambda x: functions.dict_hashnames_instances[x], max_group)

		for type2 in dict_types_instances.keys():

			intersection_bucket = len(list(set(dict_types_instances[type2]).intersection(max_bucket)))
			#intersection_group = len(list(set(functions.dict_types_instances[type2]).intersection(subject_list_group)))
			#print("Type %s containment : %d of %d" %(typez, intersection, len(types_and_instances[typez])))
			data.append([type2, len(dict_types_instances[type2]), intersection_bucket])

		print (tabulate(data, headers=["Class", "Instances", "Max_Bucket"]))
		print "------------------------------------------------------------------------------------------------------------------------------\n"




def get_types_of_instance(instance, dict_types_instances):
	types = list()

	for type in dict_types_instances.keys():
		if instance in dict_types_instances[type]:
			types.append(type)

	return types



def evaluation_type_labels(buckets, dict_types_instances):

	global number_of_instances

	precision_sum = 0
	recall_sum = 0

	for bucket in buckets:
		#print("Bucket length: %d" %len(bucket))
		dict_types_counts = {}

		for instance in bucket:
			instance_types = get_types_of_instance(instance, dict_types_instances)

			for type in instance_types:
				if type in dict_types_counts.keys():
					dict_types_counts[type] += 1
				else:
					 dict_types_counts[type] = 1


		max_count = 0

		for type in dict_types_counts.keys():
			#print("%s - %d" %(type, dict_types_counts[type]))
			if dict_types_counts[type] > max_count:
				max_count = dict_types_counts[type]
				max_type = type

			if dict_types_counts[type] == max_count:
				if len(dict_types_instances[type]) < len(dict_types_instances[max_type]):
					max_count = dict_types_counts[type]
					max_type = type

		#print("Max type = %s with count %d" %(max_type, max_count))
		#print("Number of instances of max type = %d" %len(dict_types_instances[max_type]))

		precision = len(bucket.intersection(dict_types_instances[max_type])) / len(bucket)
		recall = len(bucket.intersection(dict_types_instances[max_type])) / len(dict_types_instances[max_type])

		#print("Intersection = %d" %(len(bucket.intersection(dict_types_instances[max_type]))))
		'''print("Size of bucket: %d" %len(bucket))
		for t in dict_types_instances.keys():
			print("%s : %d / %d" %(t, len(bucket.intersection(dict_types_instances[t])), len(dict_types_instances[t])))'''

		'''print "@@@@@@@@@@@@@@@@@@@"
		print("Max type = %s" %max_type)
		print("Precision = %f" %precision)
		print("Recall = %f" %recall)
		print("f1 = %f" %f1score(precision, recall))
		print "----------------------------------------------"'''

		precision_sum += precision
		recall_sum += recall


	total_precision = precision_sum / len(buckets)
	total_recall = recall_sum / len(buckets)
	f1_score = f1score(total_precision, total_recall)

	'''f = open("results.txt", "a")
	f.write(str(total_precision) + "\t" + str(total_recall) + "\t" + str(f1_score) + "\n")
	f.write("--------------------------------------------------\n")
	f.close()'''

	#print results
	print(tabulate([	[str(total_precision), str(total_recall), str(f1_score)]],
										["Precision", "Recall", "f1_score"]))
	print "\n"

	return total_precision, total_recall, f1_score



if __name__ == '__main__':

	parser = init_parser()
	args = parser.parse_args()
	print_args_eval(args)

	bucket_file = args.bucket_file
	ground_truth_file = args.ground_truth_file
	patterns_instances_file = bucket_file.split('.')[0] + "_patterns.cnl"


	LSDisc_General.printGreen("Step 1: Reading Gold Standard file\n")
	dict_types_instances = load_gold_standard(ground_truth_file)

	LSDisc_General.printGreen("Step 2: Reading Bucket file\n")
	buckets = load_buckets(bucket_file)

	LSDisc_General.printGreen("Step 3: Reading file of Patterns and replacing with their corresponding Instances\n")
	buckets = load_patterns_instances(patterns_instances_file, buckets)

	if not args.incremental:
		#LSDisc_General.printGreen("Step 3: Evaluating Buckets\n")
		#evaluation_buckets(buckets, dict_types_instances)

		LSDisc_General.printGreen("Evaluation using type labels\n")
		evaluation_type_labels(buckets, dict_types_instances)

		#print_containment_of_max_buckets_and_groups(buckets, dict_types_instances)
	
	else:

		test_set_file = bucket_file.split(".cnl")[0] + "_test_set.cnl"
		test_set = evaluation_incremental.read_test_set(test_set_file)

		LSDisc_General.printGreen("Step 3: Evaluating Buckets\n")
		evaluation_incremental.evaluate_metric1(test_set, buckets, dict_types_instances)

		LSDisc_General.printGreen("Step 9: Overall evaluation\n")
		evaluation_incremental.evaluation_overall_metric3(buckets, dict_types_instances)




	#------------------------------------------------


	#print_containment_of_each_class_to_bucket()
	'''functions.printGreen("---------- Evaluating groups -----------------\n")
	evaluation_groups()'''
	#print_containment_of_each_class_to_group()
	#print "----------------------------------------------"
	#print_containment_of_max_buckets_and_groups()
	#evaluation_max_bucket_capacity()





#	------------------------------------------------------------------
#								GROUPS

#	store the max_containment_bucket for each type at dict_types_max_buckets
'''def find_max_group_for_type():

	#max_group_to_type = defaultdict(list) 

	for type in functions.dict_types_instances:
		max_intersection = 0
		max_group_index = 0

		#	get instances of this type
		type_instances = functions.dict_types_instances[type]

		#	get intersection with each group and store the one with max intersection
		for group in functions.groups:
			#	retrieve instances from hasnames
			subject_list = map(lambda x: functions.dict_hashnames_instances[x], group)
			intersection = len(list(set(type_instances).intersection(subject_list)))

			#	if the current group contains all the instances of the class, consider it the max_containment_group
			#	and stop checking the others
			if intersection == len(type_instances):
				max_group_index = functions.groups.index(group)
				break

			if intersection > max_intersection:
				max_intersection = intersection
				max_group_index = functions.groups.index(group)


		dict_types_max_groups[type] = max_group_index
		#max_group_to_type[max_bucket_index].append(type) '''


#	evaluation of groups -> calculate scores for each class and total scores
'''def evaluation_groups():

	#	find max_containment_group for each class
	find_max_group_for_type()

	precision_sum = 0
	recall_sum = 0

	#	list to store scores for each class
	class_scores = list()

	for type in functions.dict_types_instances.keys():

		true_positives = 0
		false_positives = 0
		false_negatives = 0

		#	get max_containment_group for this class
		max_group_index = dict_types_max_groups[type]
		group = functions.groups[max_group_index]

		#	get instance of the class
		type_instances = functions.dict_types_instances[type]

		#	retrieve instances from hasnames
		subject_list = map(lambda x: functions.dict_hashnames_instances[x], group)
		#	get intersection of class instances with max_containment_group
		true_positives = len(list(set(type_instances).intersection(subject_list)))

		if true_positives == len(type_instances):
			false_negatives = 0
		else:
			false_negatives = len(type_instances) - true_positives

		if true_positives == len(group):
			false_positives = 0
		else:
			false_positives = len(group) - true_positives

		type_precision = precision(true_positives, false_positives)
		type_recall = recall(true_positives, false_negatives)

		precision_sum += type_precision
		recall_sum += type_recall

		class_scores.append(type + "\t" + str(type_precision) + "\t" + str(type_recall) + "\t" + 
			str(f1score(type_precision, type_recall)) + "\n" )

	#	total accuracy scores
	total_precision = precision_sum / len(functions.dict_types_instances.keys())
	total_recall = recall_sum / len(functions.dict_types_instances.keys())
	f1_score = f1score(total_precision, total_recall)

	#	calculate capacity score
	capacity_score = evaluation_capacity_groups()

	#	print results
	data = [[str(total_precision), str(total_recall), str(f1_score), str(capacity_score)]]

	print (tabulate(data, headers=["Precision", "Recall", "f1_score", "capacity_score"]))
	print '\n'

	# write class scores to a file
	write_class_scores_to_file(class_scores, "groups")'''


#	calculates capacity score on groups
'''def evaluation_capacity_groups():

	sum = 0
	for group in functions.groups:

		type_to_count = {}

		#	retrieve instances from hasnames
		subject_list = map(lambda x: functions.dict_hashnames_instances[x], group)
		
		#	find how many types are contained in each group and store to dictionary type_to_count
		for type in functions.dict_types_instances.keys():
			intersection = len(list(set(functions.dict_types_instances[type]).intersection(subject_list)))
			if intersection > 0:
				type_to_count[type] = intersection

		#	calculate score for each group
		group_score = 0		
		for type in type_to_count:
			group_score +=  float(type_to_count[type] / len(functions.dict_types_instances[type]))

		if len(type_to_count.keys()) > 0:
			group_score = group_score / len(type_to_count.keys())
			sum += group_score

	#	calculate total capacity score for all groups
	capacity_score = float(sum / len(functions.groups))

	return capacity_score'''







'''def evaluation_max_bucket_capacity():

	sum = 0
	for index in max_bucket_to_type:
		bucket = buckets[index]
		types = max_bucket_to_type[index]

		type_to_count = {}
		subject_list = map(lambda x: m_to_subject[x], bucket)
		
		for type in types:
			intersection = len(list(set(types_and_instances[type]).intersection(subject_list)))
			if intersection > 0:
				type_to_count[type] = intersection

		bucket_acc = 0		
		for type in type_to_count:
			bucket_acc +=  float(type_to_count[type] / len(types_and_instances[type]))

		if len(type_to_count.keys()) > 0:
			bucket_acc = bucket_acc / len(type_to_count.keys())
			sum += bucket_acc

	accuracy = float(sum / len(max_bucket_to_type.keys()))

	print("Capacity score for max buckets: %f" %accuracy)'''

'''def print_containment_of_each_class_to_bucket():
	total_count = 0
	total_true = 0
	max_type = ""

	i = 1
	for bucket in functions.buckets:

		print("Bucket #%d: %d" %(i, len(bucket)))
		max_type_count = 0
		for typez in functions.dict_types_instances.keys():
			count = 0
			for elem in bucket:
				if functions.dict_hashnames_instances[elem] in functions.dict_types_instances[typez]:
					count += 1
			print("Type %s containment : %d of %d" %(typez, count, len(functions.dict_types_instances[typez])))

		print "----------------------------------------------------------------------------------------"

		i += 1'''


'''for bucket in functions.buckets:
		print "#################################"
		print len(bucket)
		for m in bucket:
			instance = functions.dict_hashnames_instances[m]
			for type in functions.dict_types_instances:
				if instance in functions.dict_types_instances[type]:
					if type == '<http://www.w3.org/2004/02/skos/core#Concept>':
						#print "#################################"
						#print("Bucket # %d" %idx)
						#print len(bucket)
						#listitem_buckets.add(idx)
						#print m_to_subject[m]
						print functions.dict_hashnames_properties[m]'''



'''def print_containment_of_each_class_to_group():

	i = 1
	for group in groups:

		print("Group #%d: %d" %(i, len(group)))
		for typez in types_and_instances:
			count = 0
			for elem in group:
				if m_to_subject[elem] in types_and_instances[typez]:
					count += 1
			print("Type %s containment : %d of %d" %(typez, count, len(types_and_instances[typez])))
			
		i += 1
		print "----------------------------------------------------------------------------------------"'''



'''def write_max_groups_to_file():
	f = open("max_groups.txt", "w")

	for type in types_and_instances.keys():
		f.write(type + "\n")
		max_group = type_to_max_group[type]'''
#	------------------------------------------------------------------