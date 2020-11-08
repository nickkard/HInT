# -*- coding: utf-8 -*-

from __future__ import division
from rdflib import URIRef
from rdflib import BNode
import time
import operator
import collections
import os
import errno
import random
import sys
from collections import defaultdict
from tabulate import tabulate
from datasketch import MinHash, MinHashLSH
from MyMinHashLSH import MyMinHashLSH

import LSDisc_General
#import LSDisc_RandomProjection
import Group
import Pattern

#	------------------------------------------------------------------
#	--------------- Initialization of variables ----------------------

#	LSH Family
lsh_family = "MinHash"

#	Similarity threshold
threshold = 0.5

#	Number of permutations
num_perm = 30

#	Number of hashtbles/bands (b)
#b = 42

#	Number of rows per band (r)
#r = 6

#	dictionary to store instances with no type declared
list_no_type_subjects = list()

dict_instances_properties = {}

dict_patterns_groups = defaultdict(list)

dict_buckets_type_profiles = {}

dict_patterns = defaultdict(list)

dict_types_groups = {}

stepList = list()

lsh_index = None

type_existance_probability = 0.75
#	-----------------------------------------------------------------

#	initialize LSH Index
def init_LSH(b = None, r = None):

	global lsh_index
	if lsh_family == "MinHash":
		if num_perm < 2:
			lsh_index = MyMinHashLSH(threshold=threshold, num_perm=num_perm, params=(b,r))
		else:
			lsh_index = MinHashLSH(threshold=threshold, num_perm=num_perm, params=(b,r))
		
		'''print("\tInitialized MinHashLSH with :")
		print("\t\t - Similarity threshold = %f" %threshold) 
		print("\t\t - Number of permutations = %d" %num_perm)
		print("\t\t - Number of hashtables/bands (b) = %d" %lsh_index.b)
		print("\t\t - Number of rows per band (r) = %d\n" %lsh_index.r)'''
		print("Initialized MinHashLSH Index with (b,r) = (%d, %d)" %(b,r))
	elif lsh_family == "RandomProjection":

		LSDisc_RandomProjection.init_LSH(num_perm, 2)



#	The file contains 100 lines of property sets. Each row corresponds to an instance.
def parse_dataset_from_txt_file(dataset, lsh_index):

	start_time_load = time.time()
	#	parse first file --------------------------------------------------
	f = open(dataset, "r")

	print("\tParsing dataset from .txt file and adding instances to LSH Index.\n")

	dict_instances_properties = {}
	dict_instances_hashvalues = {}

	i = 1
	#	each line represents an instance
	for line in f:

		properties = line.split('\t')
		# last element is '\r\n', remove it
		del properties[-1]

		#	remove duplicates if any
		properties = set(properties)

		properties -= {'<http://www.w3.org/2002/07/owl#sameAs>.In', \
                                '<http://www.w3.org/2000/01/rdf-schema#label>.In', \
                                '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>.In', \
                                '<http://www.w3.org/2000/01/rdf-schema#seeAlso>.In', \
                                '<http://www.w3.org/2000/01/rdf-schema#comment>.In', \
                                '<http://www.w3.org/2004/02/skos/core#subject>.In' }

		properties -= {'<http://www.w3.org/2002/07/owl#sameAs>.Out', \
                                '<http://www.w3.org/2000/01/rdf-schema#label>.Out', \
                                '<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>.Out', \
                                '<http://www.w3.org/2000/01/rdf-schema#seeAlso>.Out', \
                                '<http://www.w3.org/2000/01/rdf-schema#comment>.Out', \
                                '<http://www.w3.org/2004/02/skos/core#subject>.Out' }

		hash_value = MinHash(num_perm=num_perm)

		for prop in properties:
			hash_value.update(unicode(prop, encoding="utf8").encode('utf8'))

		lsh_index.insert(str(i), hash_value)

		#	store in dictionaries
		dict_instances_properties[str(i)] = properties
		dict_instances_hashvalues[str(i)] = hash_value

		i += 1

	end_time_load = time.time()
	print("\tDataset parsed & instances added successfully in %.04f seconds.\n" %(end_time_load - start_time_load))

	print (tabulate([[str(i-1)]], headers=["Number of Instances"]))
	print '\n'

	return dict_instances_properties, dict_instances_hashvalues
	


def load_dataset(dataset, lsh_index):

	#	Datasets DBpeedia subset and BNF are provided in txt files.
	#	No need to load to graph

	data_format = str(dataset.split('.')[-1])
	
	if data_format == 'txt':
		dict_instances_properties, dict_instances_hashvalues = parse_dataset_from_txt_file(dataset, lsh_index)
		return dict_instances_properties, dict_instances_hashvalues
	else:
		buckets, g = add_instances_to_index(dataset, lsh_index)
		return buckets, g


def hashInstance(predicates):
	hash_value = MinHash(num_perm=num_perm)
	for pred in predicates:
		# properties are encoded to utf-8 during retrieval in get_outgoing_properties and get_ingoing_properties methods
		hash_value.update(pred)

	return hash_value


#	Hash and add instances to LSH Index
def process_instances(iteration, input_dir, output_path):

	f_properties = open( input_dir + "_instances_properties.txt", "r")
	f_types = open(input_dir + "_instances_types.txt", "r")

	#f_processed = open(output_path + "_instances_processed.txt", "w")

	#f_types_SDType = open(output_path + "_types_" + str(iteration) + ".txt", "w")
	#f_untyped_SDType = open(output_path + "_untyped_instances_" + str(iteration) + ".txt", "w")

	i=1
	for line in f_properties:

		#start_time = time.time()

		line = line[:-1]
		line = line.split('\t')
		instance = line[0]
		predicates = set(line[1].split(' '))

		pattern_str = repr(sorted(predicates))
		
		'''hash_value = hashInstance(predicates)
		hv_array = hash_value.hashvalues
		hash_value_str = ''
		for x in hv_array:
			hash_value_str += str(x) + ' '
		'''

		line_types = f_types.readline()
		if LSDisc_General.considerTypeInfo():
			line_types = line_types[:-1]
			line_types = line_types.split('\t')
			if instance != line_types[0]:
				#print("Error! Instance in the same line of the two files does not match!!!")
				break

			pattern_types = set(line_types[1].split(' '))

			'''for x in pattern_types:
				f_types_SDType.write("<" + instance.decode('utf-8').encode('utf-8') + "> " + URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type').n3().encode('utf-8') +
					" <" + x.decode('utf-8').encode('utf-8') + "> .\n")'''
		else:
			pattern_types = []
			#f_untyped_SDType.write("<" + instance.decode('utf-8').encode('utf-8') + ">\n")

		#print("Instance: %s" %instance)
		if pattern_str in dict_patterns.keys():
			#print "Pattern already exists"
			existing_patterns = dict_patterns[pattern_str]

			if len(pattern_types) > 0:
				#instance_type = list(pattern_types)[0]
				handleExistingPatternWithType(pattern_types, existing_patterns, instance, predicates, pattern_str)
			else:
				handleExistingPatternWithNoType(existing_patterns, instance, predicates)
		else:
			#print "NEW PATTERN DISCOVERED"
			if len(pattern_types) > 0:
				new_pattern = Pattern.Pattern(predicates, instance, pattern_types)
			else:
				new_pattern = Pattern.Pattern(predicates, instance)
			dict_patterns[pattern_str].append(new_pattern)

			bucket = addPatternToIndex(new_pattern)
			
			'''print "BUCKET:"
			for x in bucket:
				if x.getType() == None:
					print x.getPatternNo()
				else:
					print("%d \t %s" %(x.getPatternNo(), x.getType()))'''

			if len(pattern_types) > 0:
				#instance_type = list(pattern_types)[0]
				handleNewPatternWithType(new_pattern, pattern_types, bucket)
			else:
				haldleNewPatternWithNoType(new_pattern, bucket)

		#print "---------------------------------------------------------------------------------------------------------"
		#f_processed.write(str(i) + '\n')
		#print i
		#print("Len : %d" %LSDisc_General.get_size(lsh_index))
		i += 1

		#if i % 1000000 == 0:
		#	print i

	print (tabulate([[str(i-1), len(dict_patterns_groups.keys()), len(dict_types_groups.keys())]], headers=["\nNumber of Instances", "\nNumber of Patterns", "\nTypes Found"]))
	print '\n'

	'''for x in dict_patterns.keys():
		if len(dict_patterns[x]) > 1:
			for y in dict_patterns[x]:
				print y.getType()
			print "----------------------------------------------"'''


	f_types.close()
	f_properties.close()
	#f_types_SDType.close()
	#f_untyped_SDType.close()
	#f_processed.close()


def handleExistingPatternWithType(pattern_types, existing_patterns, instance, predicates, pattern_str):
	
	#print("Types provided: %s" %pattern_types)
	found = 0
	for same_pattern in existing_patterns:
		if same_pattern.getType() == pattern_types:
			found = 1
			same_pattern.addInstance(instance)

	if found == 0:
		for same_pattern in existing_patterns:
			if same_pattern.getType() == None:
				found = 1
				same_pattern.addInstance(instance)
				updatePattern(same_pattern, pattern_types)

	if found == 0:
		new_pattern = Pattern.Pattern(predicates, instance, pattern_types)					
		dict_patterns[pattern_str].append(new_pattern)

		bucket = addPatternToIndex(new_pattern)
		#print "BUCKET:"
		#for x in bucket:
		#	print x.getPatternNo()

		for instance_type in pattern_types:
			if instance_type in dict_types_groups.keys():
				type_group = dict_types_groups[instance_type]
				dict_patterns_groups[new_pattern].append(type_group)
				#print("Group No %d has already been assigned type '%s'" %(type_group.getGroupNo(), instance_type))
				updateGroupWithBucket(new_pattern, type_group, instance_type, bucket)
			else:
				new_group = Group.Group(new_pattern, instance_type)
				dict_patterns_groups[new_pattern].append(new_group)
				dict_types_groups[instance_type] = new_group
				updateGroupWithBucket(new_pattern, new_group, instance_type, bucket)



def handleExistingPatternWithNoType(existing_patterns, instance, predicates):
	#print "Type not provided"
	'''for same_pattern in existing_patterns:
		print("Adding instance to pattern no : %d of group %d" %(same_pattern.getPatternNo(), dict_patterns_groups[same_pattern].getGroupNo()))
		same_pattern.addInstance(instance)'''

	if len(existing_patterns) == 1:
		same_pattern = existing_patterns[0]
		#print("Adding instance to pattern no : %d" %same_pattern.getPatternNo())
		same_pattern.addInstance(instance)
	else:
		#for same_pattern in existing_patterns:
		'''same_pattern = existing_patterns[0]
		print("Adding instance to pattern no : %d" %same_pattern.getPatternNo())
		same_pattern.addInstance(instance)'''

		'''for p in existing_patterns:
			existing_patterns_types = ""
			for t in p.getType():
				existing_patterns_types += t + ' '
			print("%d : %s" %(p.getPatternNo(), existing_patterns_types))'''

		max_js = 0
		dict_sim_type = defaultdict(list)
		types_in_existing_patterns = list()
		for same_pattern in existing_patterns:
			#print("Type not provided. Adding instance to pattern no : %d of group %d" %(same_pattern.getPatternNo(), dict_patterns_groups[same_pattern].getGroupNo()))
			#same_pattern.addInstance(instance)

			for t in same_pattern.getType():
				if t not in types_in_existing_patterns:
					types_in_existing_patterns.append(t)
					group_of_type = dict_types_groups[t]
					type_preds = set()
					for p in group_of_type.getSetOfPatterns():
						type_preds = type_preds.union(p.getSetOfProperties())

					js = LSDisc_General.jaccard_similarity(predicates, type_preds)
					dict_sim_type[js].append(t)
					if js > max_js:
						max_js = js

					type_preds.clear()

		#print "Computing similarities..."
		'''for x in dict_sim_type.keys():
			print x
			print dict_sim_type[x]
			print "-------------------------------------------------------------------"'''

		if len(dict_sim_type[max_js]) == 1:
			max_type = dict_sim_type[max_js][0]
			#print max_type
			#print("Only type '%s' has max similarity" %max_type)
			for same_pattern in existing_patterns:
				if max_type in same_pattern.getType():
					#print same_pattern.getType()
					#print("Adding instance to pattern no : %d" %same_pattern.getPatternNo())
					same_pattern.addInstance(instance)
			#print "---"
			'''dict_len_same_pattern = defaultdict(list)
			min_len = 100000000
			for same_pattern in existing_patterns:
				if max_type in same_pattern.getType():
					dict_len_same_pattern[len(same_pattern.getType())].append(same_pattern)
					if len(same_pattern.getType()) < min_len:
						min_len = len(same_pattern.getType())'''

			'''print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
			print "dict_len_same_pattern:"
			for length in dict_len_same_pattern.keys():
				pat = ""
				for p in dict_len_same_pattern[length]:
					pat += str(p.getPatternNo()) + ' '
				print("%s : %s" %(length, pat))
			print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"'''

			'''if len(dict_len_same_pattern[min_len]) == 1:
				#print("only one pattern has min len")
				#print("Adding instance to pattern no : %d" %dict_len_same_pattern[min_len][0].getPatternNo())
				dict_len_same_pattern[min_len][0].addInstance(instance)
			else:
				#print "Mupltiple patterns have min len"
				#print("Adding instance to pattern no : %d" %dict_len_same_pattern[min_len][0].getPatternNo())
				dict_len_same_pattern[min_len][0].addInstance(instance)

			dict_len_same_pattern.clear()'''
		else:
			#print "Multiple types have maximum similarity:"
			#print dict_sim_type[max_js]

			#print "Calculating counts..."
			dict_types_patterns = defaultdict(list)
			for same_pattern in existing_patterns:
				for t in dict_sim_type[max_js]:
					if t in same_pattern.getType(): 
						dict_types_patterns[t].append(same_pattern)

			'''print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
			print "dict_types_patterns:"
			for t in dict_types_patterns.keys():
				pat = ""
				for p in dict_types_patterns[t]:
					pat += str(p.getPatternNo()) + ' '
				print("%s : %s" %(t, pat))
			print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"'''


			max_count = 0
			dict_count_types = defaultdict(list)
			for t in dict_types_patterns.keys():
				dict_count_types[len(dict_types_patterns[t])].append(t)
				if len(dict_types_patterns[t]) > max_count:
					max_count = len(dict_types_patterns[t])

			'''print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
			print "dict_count_types:"
			for count in dict_count_types.keys():
				typ = ""
				for t in dict_count_types[count]:
					typ += t + ' '
				print("%s : %s" %(count, typ))
			print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"'''

			types_with_max_count = dict_count_types[max_count]
			#print "Types with max count:"
			#print types_with_max_count

			if len(types_with_max_count) == 1:
				type_with_max_count = types_with_max_count[0]
				#print("Only type %s has max count!" %type_with_max_count)
				exists = 0
				for same_pattern in existing_patterns:
					if same_pattern.getType() == {type_with_max_count}:
						exists = 0
						#print("Pattern no %d has exact same types as types with max count!" %same_pattern.getPatternNo())
						same_pattern.addInstance(instance)

				if exists == 0:
					#print "There is no pattern in existing pattern with that exact type!"
					dict_len_same_pattern = defaultdict(list)
					min_len = 100000000
					for same_pattern in existing_patterns:
						if type_with_max_count in same_pattern.getType():
							dict_len_same_pattern[len(same_pattern.getType())].append(same_pattern)
							if len(same_pattern.getType()) < min_len:
								min_len = len(same_pattern.getType())

					'''print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
					print "dict_len_same_pattern:"
					for length in dict_len_same_pattern.keys():
						pat = ""
						for p in dict_len_same_pattern[length]:
							pat += str(p.getPatternNo()) + ' '
						print("%s : %s" %(length, pat))
					print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"'''

					if len(dict_len_same_pattern[min_len]) == 1:
						#print("only one pattern has min len")
						#print("Adding instance to pattern no : %d" %dict_len_same_pattern[min_len][0].getPatternNo())
						dict_len_same_pattern[min_len][0].addInstance(instance)
					else:
						#print "Mupltiple patterns have min len"
						#print("Adding instance to pattern no : %d" %dict_len_same_pattern[min_len][0].getPatternNo())
						dict_len_same_pattern[min_len][0].addInstance(instance)

					dict_len_same_pattern.clear()

			else:
				#print "Multiple types have max count!"
				'''random_type = types_with_max_count[0]
				for same_pattern in existing_patterns:
					if random_type in same_pattern.getType():
						print("Adding instance to pattern no : %d" %same_pattern.getPatternNo())
						same_pattern.addInstance(instance)
						break'''
				types_with_max_count_set = set()
				for t in types_with_max_count:
					types_with_max_count_set.add(t)
				exists = 0
				for same_pattern in existing_patterns:
					if same_pattern.getType() == types_with_max_count_set:
						exists = 1
						#print("Pattern no %d has exact same types as types with max count!" %same_pattern.getPatternNo())
						same_pattern.addInstance(instance)

				if exists == 0:
					#print "There is no pattern in existing pattern with that exact type!"

					for t in types_with_max_count_set:
						#print("Type : %s" %t)
						dict_len_same_pattern = defaultdict(list)
						min_len = 100000000
						for same_pattern in existing_patterns:
							if t in same_pattern.getType():
								dict_len_same_pattern[len(same_pattern.getType())].append(same_pattern)
								if len(same_pattern.getType()) < min_len:
									min_len = len(same_pattern.getType())

						'''print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
						print "dict_len_same_pattern:"
						for length in dict_len_same_pattern.keys():
							pat = ""
							for p in dict_len_same_pattern[length]:
								pat += str(p.getPatternNo()) + ' '
							print("%s : %s" %(length, pat))
						print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"'''

						if len(dict_len_same_pattern[min_len]) == 1:
							#print("only one pattern has min len")
							#print("Adding instance to pattern no : %d" %dict_len_same_pattern[min_len][0].getPatternNo())
							dict_len_same_pattern[min_len][0].addInstance(instance)
						else:
							#print "Mupltiple patterns have min len"
							#print("Adding instance to pattern no : %d" %dict_len_same_pattern[min_len][0].getPatternNo())
							dict_len_same_pattern[min_len][0].addInstance(instance)

						dict_len_same_pattern.clear()

				types_with_max_count_set.clear()
			dict_count_types.clear()
			dict_types_patterns.clear()
		dict_sim_type.clear()





def handleNewPatternWithType(new_pattern, pattern_types, bucket):

	for instance_type in pattern_types:
		if instance_type in dict_types_groups.keys():
			type_group = dict_types_groups[instance_type]
			dict_patterns_groups[new_pattern].append(type_group)
			#print("Group No %d has already been assigned type '%s'" %(type_group.getGroupNo(), instance_type))
			updateGroupWithBucket(new_pattern, type_group, instance_type, bucket)
		else:
			new_group = Group.Group(new_pattern, instance_type)
			dict_patterns_groups[new_pattern].append(new_group)
			dict_types_groups[instance_type] = new_group
			updateGroupWithBucket(new_pattern, new_group, instance_type, bucket)



def haldleNewPatternWithNoType(new_pattern, bucket):
	#print "No type provided"
	new_group = Group.Group(new_pattern)
	dict_patterns_groups[new_pattern].append(new_group)

	dict_types_count = defaultdict(list)
	for p in bucket:
		if p.getType() != None:
			for t in p.getType():
				dict_types_count[t].append(p)

	if len(dict_types_count.keys()) == 0:
		#print "No type present in bucket"

		for p in bucket:
			new_group.addPattern(p)
			#print("Adding pattern no %d to group no %d " %(p.getPatternNo(), new_group.getGroupNo()))
			for g in dict_patterns_groups[p]:
				g.addPattern(new_pattern)
				#print("Adding pattern no %d to group no %d " %(new_pattern.getPatternNo(), g.getGroupNo()))

	else:
		#print "Types present in bucket:"
		#print dict_types_count.keys()

		types_with_max_patterns = findTypesWithMaxPatterns(dict_types_count)
		#print "Types with max number of patterns:"
		#print types_with_max_patterns
		
		if len(types_with_max_patterns) == 1:
			type_with_max_patterns = types_with_max_patterns[0]
			#print("Only type '%s' is present in bucket" %type_with_max_patterns)
			new_group.setType(type_with_max_patterns)
			if type_with_max_patterns in dict_types_groups.keys():
				type_group = dict_types_groups[type_with_max_patterns]
				#print("Group no %d with same type already exists" %type_group.getGroupNo())
				updateGroupWithBucket(new_pattern, type_group, type_with_max_patterns, bucket)
				dict_patterns_groups[new_pattern].remove(new_group)
				dict_patterns_groups[new_pattern].append(type_group)
				#print("Deleting group no %d" %new_group.getGroupNo())
				del new_group
			else:
				for p in bucket:
					new_group.addPattern(p)
					#print("Adding pattern no %d to group no %d " %(p.getPatternNo(), new_group.getGroupNo()))
					for g in dict_patterns_groups[p]:
						g.addPattern(new_pattern)
						#print("Adding pattern no %d to group no %d " %(new_pattern.getPatternNo(), g.getGroupNo()))

				dict_types_groups[type_with_max_patterns] = new_group
		else:
			#print("Multiple types are present in bucket:")

			max_type = findTypeWithMaxSimilarity(types_with_max_patterns, dict_types_count, new_pattern)
			#print("Type with max similarity with pattern : %s" %max_type)
			type_group = dict_types_groups[max_type]
			dict_patterns_groups[new_pattern].remove(new_group)
			dict_patterns_groups[new_pattern].append(type_group)
			updateGroupWithBucket(new_pattern, type_group, max_type, bucket)
			#print("Deleting group no %d" %new_group.getGroupNo())
			del new_group

	dict_types_count.clear()

	'''if len(dict_types_count.keys()) == 0:
		print "No type present in bucket"

		for p in bucket:
			new_group.addPattern(p)
			print("Adding pattern no %d to group no %d " %(p.getPatternNo(), new_group.getGroupNo()))
			for g in dict_patterns_groups[p]:
				g.addPattern(new_pattern)
				print("Adding pattern no %d to group no %d " %(new_pattern.getPatternNo(), g.getGroupNo()))

	elif len(dict_types_count.keys()) == 1:
		type_present = dict_types_groups.keys()[0]
		print("Only type '%s' is present in bucket" %type_present)

		type_group = dict_types_groups[type_present]
		print("Group no %d with same type already exists" %type_group.getGroupNo())
		updateGroupWithBucket(new_pattern, type_group, type_present, bucket)
		dict_patterns_groups[new_pattern].remove(new_group)
		dict_patterns_groups[new_pattern].append(type_group)
		print("Deleting group no %d" %new_group.getGroupNo())
		del new_group

	else:
		print("Multiple types are present in bucket:")
		print dict_types_count.keys()

		max_types = findTypesWithMaxSimilarity(dict_types_count, new_pattern)
		print("Types with max similarity with pattern : %s" %max_types)

		if len(max_types) == 1:
			type_with_max_sim = max_types[0]
			print("Type '%s' has max similarity" %type_with_max_sim)
			#if type_with_max_patterns in dict_types_groups.keys():
			type_group = dict_types_groups[type_with_max_sim]
			print("Group no %d with same type already exists" %type_group.getGroupNo())
			updateGroupWithBucket(new_pattern, type_group, type_with_max_sim, bucket)
			dict_patterns_groups[new_pattern].remove(new_group)
			dict_patterns_groups[new_pattern].append(type_group)
			print("Deleting group no %d" %new_group.getGroupNo())
			del new_group

		else:
			print("Multiple types have max similarity!")
			print max_types

			print "Counting occurances of types..."
			types_with_max_patterns = findTypesWithMaxPatterns(dict_types_count)
			print "Types with max number of patterns:"
			print types_with_max_patterns
		
			if len(types_with_max_patterns) == 1:
				type_with_max_patterns = types_with_max_patterns[0]
				print("Only Type '%s' has max patterns" %type_with_max_patterns)
				#if type_with_max_patterns in dict_types_groups.keys():
				type_group = dict_types_groups[type_with_max_patterns]
				print("Group no %d with same type already exists" %type_group.getGroupNo())
				updateGroupWithBucket(new_pattern, type_group, type_with_max_patterns, bucket)
				dict_patterns_groups[new_pattern].remove(new_group)
				dict_patterns_groups[new_pattern].append(type_group)
				print("Deleting group no %d" %new_group.getGroupNo())
				del new_group
			else:
				print "Multiple types have max patterns!"

				random_type_with_max_patterns = types_with_max_patterns[0]
				type_group = dict_types_groups[random_type_with_max_patterns]
				dict_patterns_groups[new_pattern].remove(new_group)
				dict_patterns_groups[new_pattern].append(type_group)
				updateGroupWithBucket(new_pattern, type_group, random_type_with_max_patterns, bucket)
				print("Deleting group no %d" %new_group.getGroupNo())
				del new_group'''





def updatePattern(pattern, pattern_types):
	if pattern.getType() == None:
		#print("Updating pattern no %d by setting types as %s" %(pattern.getPatternNo(), pattern_types))
		pattern.setType(pattern_types)

		pattern_groups = dict_patterns_groups[pattern]
		if len(pattern_types) == 1:
			instance_type = list(pattern_types)[0]
			#print "Pattern has 1 type"
			if len(pattern_groups) > 1:
				LSDisc_General.printRed("THERE ARE MULTIPLE GROUPS!")
			pattern_group = pattern_groups[0]
			#print("pattern_group.getGroupNo() : %d" %pattern_group.getGroupNo())
			if pattern_group.getType() == None:
				updateGroup(pattern_group, instance_type, None)
				updateGroupsOfSimilarPatterns(pattern, instance_type, pattern_types)
			elif pattern_group.getType() != instance_type:
				pattern_group.removePattern(pattern)

				if instance_type in dict_types_groups.keys():
					type_group = dict_types_groups[instance_type]
					type_group.addPattern(pattern)
					dict_patterns_groups[pattern].remove(pattern_group)
					dict_patterns_groups[pattern].append(type_group)
					'''for p in pattern_group.getSetOfPatterns():
						if p.getType() == None:
							type_group.addPattern(p)'''
				else:
					new_group = Group.Group(pattern, instance_type)
					dict_patterns_groups[pattern].append(new_group)
					dict_patterns_groups[pattern].remove(pattern_group)
					dict_types_groups[instance_type] = new_group
					'''for p in pattern_group.getSetOfPatterns():
						if p.getType() == None:
							new_group.addPattern(p)'''

		else:
			#print "Pattern has multiple types"
			for instance_type in pattern_types:
				#print("Type: %s" %instance_type)
				updated = 0
				for group in pattern_groups:
					if group.getType() == None:
						#print("Group %d has type = None" %group.getGroupNo())
						updateGroup(group, instance_type, None)
						updateGroupsOfSimilarPatterns(pattern, instance_type, pattern_types)
						updated = 1
						break

				if updated == 0:
					if instance_type in dict_types_groups.keys():
						#print("Type already discovered")
						dict_types_groups[instance_type].addPattern(pattern)
						if dict_types_groups[instance_type] not in pattern_groups:
							dict_patterns_groups[pattern].append(dict_types_groups[instance_type])
					else:
						#print "New type found"
						new_group = Group.Group(pattern, instance_type)
						dict_patterns_groups[pattern].append(new_group)
						dict_types_groups[instance_type] = new_group
						updateGroupsOfSimilarPatterns(pattern, instance_type, pattern_types)

		


def updateGroupsOfSimilarPatterns(same_pattern, instance_type, pattern_types):
	#print("Updating similar patterns of pattern no %d" %same_pattern.getPatternNo())
	to_add = list()
	for g in dict_patterns_groups[same_pattern]:
		for pattern in g.getSetOfPatterns():
			if pattern != same_pattern:
				groups = dict_patterns_groups[pattern]
				for group in groups:
					if group not in dict_patterns_groups[same_pattern]:
						if group.getType() != None and group.getType() not in pattern_types and same_pattern in group.getSetOfPatterns():
							#print instance_type
							#print("Removing pattern no %d from group no %d, which has type: '%s'" %(same_pattern.getPatternNo(), group.getGroupNo(), group.getType()))
							group.removePattern(same_pattern)
						else:
							patterns_to_add = updateGroup(group, instance_type, "updateGroupsOfSimilarPatterns")
							for x in patterns_to_add:
								to_add.append(x)
	for p in to_add:
		dict_types_groups[instance_type].addPattern(p)




def updateGroupWithBucket(pattern, group, instance_type, bucket):
	for p in bucket:
		if p.getType() == None or instance_type in p.getType():
			group.addPattern(p)
			#print("Adding pattern no %d to group no %d with type '%s'" %(p.getPatternNo(), group.getGroupNo(), instance_type))
		if p.getType() == None:
			for g in dict_patterns_groups[p]:
				if g.getType() == None:
					g.addPattern(pattern)
					#print("Adding pattern no %d to group no %d " %(pattern.getPatternNo(), g.getGroupNo()))
					if p != pattern:
						updateGroup(g, instance_type, None)




def updateGroup(group, instance_type, callerName):
	to_return = list()
	if group.getType() == None:
		#print("Updating group No %d by setting type to %s" %(group.getGroupNo(), instance_type))
		group.setType(instance_type)
		if instance_type in dict_types_groups.keys():
			#print("Group no %d with same type already exists" %dict_types_groups[instance_type].getGroupNo())
			for p in group.getSetOfPatterns():
				#print("Adding pattern no %d to group no %d" %(p.getPatternNo(), dict_types_groups[instance_type].getGroupNo()))

				if callerName == "updateGroupsOfSimilarPatterns":
					to_return.append(p)
				else:
					dict_types_groups[instance_type].addPattern(p)
				if group in dict_patterns_groups[p]:
					dict_patterns_groups[p].append(dict_types_groups[instance_type])
					dict_patterns_groups[p].remove(group)
					#print dict_patterns_groups[p]
			#print("Deleting group no %d" %group.getGroupNo())
			del group
		else:
			dict_types_groups[instance_type] = group

	if callerName == "updateGroupsOfSimilarPatterns":
		return to_return




def findTypesWithMaxPatterns(dict_types_count):
	max_count = 0
	for t in dict_types_count.keys():
		if len(dict_types_count[t]) > max_count:
			max_count = len(dict_types_count[t])

	count = 0
	types_with_max_patterns = list()
	for t in dict_types_count.keys():
		if len(dict_types_count[t]) == max_count:
			count += 1
			types_with_max_patterns.append(t)

	return types_with_max_patterns



def findTypeWithMaxSimilarity(types_with_max_patterns, dict_types_count, pattern):
	max_sim = 0
	for t in types_with_max_patterns:
		properties = set()
		for p in dict_types_count[t]:
			properties = properties.union(p.getSetOfProperties())
		js = LSDisc_General.jaccard_similarity(properties, pattern.getSetOfProperties())
		#print("Type '%s' with similarity %f" %(t, js))
		if js > max_sim:
			max_sim = js
			max_type = t

		properties.clear()

	return max_type




def findTypesWithMaxSimilarity(dict_types_count, pattern):
	
	dict_sim_types = defaultdict(list)
	max_sim = 0
	for t in dict_types_count.keys():
		properties = set()
		for p in dict_types_count[t]:
			properties = properties.union(p.getSetOfProperties())
		js = LSDisc_General.jaccard_similarity(properties, pattern.getSetOfProperties())
		dict_sim_types[js].append(t)
		#print("Type '%s' with similarity %f" %(t, js))
		if js > max_sim:
			max_sim = js

		properties.clear()

	max_types = dict_sim_types[max_sim]
	#print("Types with max similarity: ")
	#print max_types
	return max_types



def addPatternToIndex(pattern):
	predicate_list = pattern.getSetOfProperties()

	hash_value = MinHash(num_perm=num_perm)

	for pred in predicate_list:
		# properties are encoded to utf-8 during retrieval in get_outgoing_properties and get_ingoing_properties methods
		hash_value.update(pred)

	lsh_index.insert(pattern, hash_value)

	return set(lsh_index.query(hash_value))



def clearDictionaries():
	dict_patterns_groups.clear()
	dict_types_groups.clear()
	global dict_patterns
	dict_patterns = defaultdict(list)
	global lsh_index
	del lsh_index



def rebuildIndex(b, r, tuning, stepList):
	new_b, new_r = ParameterTuning(b, r, tuning, stepList)
	dict_patterns_groups.clear()
	print("size of dict = %d" %len(dict_patterns_groups))
	global lsh_index
	del lsh_index
	global groupNo
	groupNo = 0
	UpdateIndex(new_b, new_r, 0, stepList)



def ParameterTuning(b, r, tuning, stepList):
	print("~~~~~~~~~~~~~~~~~~~~~~~~~~ Parameter Tuning : %s ~~~~~~~~~~~~~~~~~~~~~~~~~~" %tuning)
	previousStep = stepList[-1]
	print("previous step = %f" %previousStep)
	step = pow(1/b, 1/r)
	print("step = %f" %step)
	temp = step

	#	Find new step
	if tuning == "increaseStep":
		if previousStep <= step:	# two successive increments
			step = step + (1-step)/2
		else:
			step = (step + previousStep)/2
	elif tuning == "decreaseStep":
		if previousStep >= step:	#two successive decrements
			step = step/2
		else:
			step = (step + previousStep)/2

	print("New step : %f" %step)
	stepList.append(step)

	#	Find b and r for new step. Try to minimize b for performance
	f = float('inf')
	temp_b = 1
	while temp_b <= num_perm:
		x = temp_b/num_perm
		temp = abs(pow(1/temp_b, x) - step)
		print("%d, %f =  %f " %(temp_b, x, temp))

		if temp < f :
			f = temp
			new_b = temp_b
		temp_b += 1

	print("New values of (b,r) : (%d, %d)" %(new_b, int(num_perm/new_b)))
	step = pow(1/new_b, 1/int(num_perm/new_b))
	print("step = %f" %step)
	print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	return new_b, int(num_perm/new_b)




def UpdateIndex(b, r, pattern_to_insert, stepList):

	#	rebuild index from scratch  with all patterns
	if pattern_to_insert == 0:
		patternsToInsert = dict_patterns.keys()
		init_LSH(b,r)

	#	add new pattern to index
	else:
		print("Updating Index with pattern no %d" %dict_patterns[pattern_to_insert].getPatternNo())
		patternsToInsert = [pattern_to_insert]

	for pattern_str in patternsToInsert:

		pattern = dict_patterns[pattern_str]

		predicate_list = pattern.getSetOfProperties()

		hash_value = MinHash(num_perm=num_perm)

		for pred in predicate_list:
			# properties are encoded to utf-8 during retrieval in get_outgoing_properties and get_ingoing_properties methods
			hash_value.update(pred)

		lsh_index.insert(pattern, hash_value)

		bucket = set(lsh_index.query(hash_value))
		print("Bucket len: %d"%len(bucket))
		print bucket

		print("Pattern No %d added to LSH Index" %pattern.getPatternNo())

		#	build new group
		group = Group.Group(pattern)

		global groupNo
		groupNo += 1
		group.setGroupNo(groupNo)
		dict_patterns_groups[pattern] = group

		print("[NEW] Adding new Group with No %d" %groupNo)

		if pattern.getTypes() != None:
			print("Setting the type of Group no: %d as %s" %(group.getGroupNo(), pattern.getTypes()))
			group.setTypes(pattern.getTypes())

		for p in bucket:
			group.addPattern(p)
			if p.getTypes() != None:
			 	if group.getTypes() == None:
					print("Setting group No %d type as : %s" %(group.getGroupNo(), p.getTypes()))
					group.setTypes(p.getTypes())
				elif group.getTypes() != p.getTypes():
						print("Pattern No %d contained in Group No %d has different type!" %(p.getPatternNo(), group.getGroupNo()))
						print("\t Pattern No %d Type : %s" %(p.getPatternNo(), p.getTypes()))
						print("\t Group No %d Type : %s" %(group.getGroupNo(), group.getTypes()))
						print("Rebuilding Index with tunin = IncreseStep!!!!")
						rebuildIndex(b, r, "increaseStep", stepList)
						break

		print("Update groups of patterns in Group No %d" %group.getGroupNo())
		for p in group.getSetOfPatterns():
			if p != pattern:
				g = dict_patterns_groups[p]
				if g != group:
					g.addPattern(pattern)
					if pattern.getTypes() != None: # and g.getTypes() == None:
						#g.setTypes(pattern.getTypes())						
						if pattern.getTypes() == g.getTypes():
							print("Pattern No %d and Group No %d have same types!" %(pattern.getPatternNo(), g.getGroupNo()))
							print("\t Pattern No %d Type : %s" %(pattern.getPatternNo(), pattern.getTypes()))
							print("\t Group No %d Type : %s" %(g.getGroupNo(), g.getTypes()))
							print("Rebuilding Index with tunin = DecreseStep!!!!")
							rebuildIndex(b, r, "decreaseStep", stepList)
							break
						else:
							if g.getTypes() == None:
								g.setTypes(pattern.getTypes())
								# and ..... ?
							elif g.getTypes() != pattern.getTypes():
								print("Pattern No %d and Group No %d have different types!" %(pattern.getPatternNo(), g.getGroupNo()))
								print("\t Pattern No %d Type : %s" %(pattern.getPatternNo(), pattern.getTypes()))
								print("\t Group No %d Type : %s" %(g.getGroupNo(), g.getTypes()))
								print("Rebuilding Index with tunin = IncreseStep!!!!")
								rebuildIndex(b, r, "increaseStep", stepList)
								break
		
		'''found = 0
		for group in dict_patterns_groups.values():
			if group.getSetOfPatterns() == bucket - {pattern}:
				found = 1
				#print("Adding Pattern No %d to Group No %d with type %s" %(pattern.getPatternNo(), group.getGroupNo(), group.getType()))
				print("Adding Pattern No %d to Group No %d " %(pattern.getPatternNo(), group.getGroupNo()))
				group.addPattern(pattern)

				dict_patterns_groups[pattern] = group

				if pattern.getType() != None:
					if group.getType() == None:
						group.setType(pattern.getType())
					elif group.getType() != pattern.getType():
						print("Pattern No %d contained in Group No %d have different types!" %(pattern.getPatternNo(), group.getGroupNo()))
						print("\t Pattern No %d Type : %s" %(pattern.getPatternNo(), pattern.getType()))
						print("\t Group No %d Type : %s" %(group.getGroupNo(), group.getType()))
						print("Rebuilding Index with tuning = IncreseStep!!!!")
						rebuildIndex(b, r, "increaseStep", stepList)
						break
				break'''

		'''if found == 0:
			#	build group
			group = Group.Group(pattern)

			global groupNo
			groupNo += 1
			group.setGroupNo(groupNo)
			dict_patterns_groups[pattern] = group

			print("[NEW] Adding new Group with No %d" %groupNo)'''


		'''for p in bucket:
				if p != pattern and p.getType() != None:
					if group.getType() == None:
						print("Setting group No %d type as : %s" %(group.getGroupNo(), p.getType()))
						group.setType(p.getType())
					elif group.getType() != p.getType():
						print("Pattern No %d contained in Group No %d has different type!" %(p.getPatternNo(), group.getGroupNo()))
						print("\t Pattern No %d Type : %s" %(p.getPatternNo(), p.getType()))
						print("\t Group No %d Type : %s" %(group.getGroupNo(), group.getType()))
						print("Rebuilding Index with tunin = IncreseStep!!!!")
						rebuildIndex(b, r, "increaseStep", stepList)
						break'''




#	create buckets
def create_buckets(dict_instances_hashvalues, lsh_index):

	global groups
	
	start = time.time()

	buckets = list()

	#	query the index with each unique instance to retrieve the bucket it is classified to
	for query_point in dict_instances_hashvalues.keys():
		bucket = set(lsh_index.query(dict_instances_hashvalues[query_point]))

		#	add the bucket returned in the set of buckets if it not already contained
		if bucket not in buckets:
			buckets.append(bucket)
	
		#	------------------------------------------------------------------------------------------------------------------
		#	Code in dashed lines is used to create groups

		#	NOTE: set() is needed (instead of list) in order to compare groups or buckets.
		#	list comparison takes ordering into account, so ['1', '2'] != ['2', '1']. Set does not!
		#	use filter() to keep instances from the bucket that have jaccard similarity >= threshold with the query point
		'''group_above_threshold = set(filter(lambda x: (x != query_point) and jaccard_similarity(dict_instances_properties[x], dict_instances_properties[query_point]) >= threshold, bucket))
		group_above_threshold.add(query_point)

		#group_below_threshold = bucket.difference(group_above_threshold)

		if group_above_threshold not in groups:
			groups.append(group_above_threshold)'''

		#if group_below_threshold not in groups and len(group_below_threshold) > 0:
		#	groups.append(group_below_threshold)

		#print("# of groups : %d" %len(groups))
		#---------------------------------------------------------------------------------------------------------------------
	
	print("\tNumber of buckets generated: %d\n" %len(buckets)) 

	#	------------------------------------------------------------------------------------------------------------------
	#	Code in dashed lines is used to merge groups

	#	call merge_groups() function in order to merge groups
	#merged_groups = merge_groups(groups)
	
	#print("# of merged groups: %d" %len(merged_groups))'''
	#---------------------------------------------------------------------------------------------------------------------

	end = time.time()

	print("\tTime needed for bucket generation = %.4f seconds.\n" %(end-start))

	return buckets




#	create type profile for each bucket
#	each type profile has the form: (dictionary, counter), where dictionary has the form {prop1: probab1, prop2: probab2, ....}
#	and counter represents the number of instances that have contributed to the type profile
def create_type_profiles(buckets):

	list_type_profiles = list()

	for id_bucket,bucket in enumerate(buckets):
		type_profile = {}

		#	get the properties of each instance in bucket
		for m in bucket:
			properties = dict_instances_properties[m]

			#	if a property is already contained in the dictionary, just update the counter
			#	else, add the property with counter = 1
			for prop in properties:
				if prop in type_profile.keys():
					type_profile[prop] += 1
				else:
					type_profile[prop] = 1

		#	compute the probability of each property by dividing each property count with the number of elements in the bucket
		type_profile = dict(map(lambda x: (x[0], x[1] / len(bucket)), type_profile.items()))

		'''print " --------------------------------------------------------------------------"
		for x in type_profile.keys():
			print("%s - %f" %(x, type_profile[x]))'''

		#	store the bucket id and its type profile to dict_buckets_type_profiles
		#dict_buckets_type_profiles[id_bucket] = (type_profile, len(bucket)) 

		#	store type profile to global list
		list_type_profiles.append((type_profile, len(bucket)))


	global dataset_global
	#	write type profiles to file
	#general_functions.output_type_profiles(dataset_global, list_type_profiles)



#	--------------------- GROUPS -------------------------------------------

#	Find the pair with maximum similarity across groups.
#	Return value: max similarity and the corresponding pair of groups
#	If the maximum similarity found is below threshold, 0 is returned.
def find_max_similarity():

	max_sim = 0
	max_sim_pair = ()
	for id1, group1 in enumerate(groups):
		for id2, group2 in enumerate(groups):
			if id1 != id2:
				sim = general_functions.jaccard_similarity(group1, group2)
				if sim >= threshold and sim > max_sim:
					max_sim = sim
					max_sim_pair = (group1, group2)

	return max_sim , max_sim_pair



#	Merge groups according to their similarity
def merge_groups(groups):
	merged_groups = list()

	#	find pair with maximum similarity
	max_sim, max_sim_pair = find_max_similarity()

	#	repeat as long as the maximum similarity between pairs is not 0 (0 is equivalent to maximum similarity below threshold -> see function above)
	while max_sim != 0:
		print("maximum similarity found = %f" %max_sim)

		#	merge individual groups into a signle one
		groups.append(max_sim_pair[0].union(max_sim_pair[1]))
		#	and remove the individuals
		groups.remove(max_sim_pair[0])
		groups.remove(max_sim_pair[1])
		print("*** Merging group %d with %d in new group: %s" %(len(max_sim_pair[0]), len(max_sim_pair[1]), len(max_sim_pair[0].union(max_sim_pair[1]))))

		max_sim, max_sim_pair = find_max_similarity()
	return groups


	'''similarity_dict = {}

	for id1, group1 in enumerate(groups):
		for id2, group2 in enumerate(groups):
			if id1 != id2:
				sim = jaccard_similarity(group1, group2)
				if sim >= threshold:
					if (group1, group2) not in similarity_dict.keys() and (group2, group1) not in similarity_dict.keys():
						similarity_dict[(list(group1), list(group2))] = sim


	print len(similarity_dict.keys())'''


	'''for id1, group1 in enumerate(groups):
		for id2, group2 in enumerate(groups):
			if id1 != id2:
				if jaccard_similarity(group1, group2) >= threshold:
					merged_groups.append(group.union(group2)) 
					print("Merging group %d with %d" %(len(group1), len(group2)))
				else:
					merged_groups.append(group1)
					merged_groups.append(group2)'''
