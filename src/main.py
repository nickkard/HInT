import sys
import os
import LSDisc_MinHash
import LSDisc_RandomProjection
import LSDisc_General
import LSDisc_Incremental
import evaluation
from tabulate import tabulate



#	datasets and formats
'''datasets = [ 	("../Datasets/Conference/dc-2010-complete.rdf", "rdf"),
				("../Datasets/swdf/clean_swdf.nt", "nt"),
				("statix/opengov/schools.nt", "nt"),
				("statix/opengov/bauhist-fotosamm.nt", "nt"),
				("statix/opengov/hist_munic_reg.nt", "nt"),
				("statix/kenzabased/museum.rdf", "nt"),
				("statix/kenzabased/soccerplayer.rdf", "nt"),
				("statix/kenzabased/country.rdf", "nt"),
				("statix/kenzabased/politician.rdf", "nt"),
				("statix/kenzabased/film.rdf", "nt")
			]'''

if __name__ == '__main__':

	parser = LSDisc_General.init_parser()
	args = parser.parse_args()
	LSDisc_General.print_args(args)

	dataset = str(args.input_file)

	b = 4
	r = 7
	iterations = 1

	if not args.incremental:
		if args.hash_function == None or args.hash_function[0] == "mh":

			input_dirs = dataset.split('/')[:-1]
			dataset = input_dirs[-1]
			input_dir = ''
			for dir in input_dirs:
				input_dir += dir + '/'
			input_dir += str(dataset)

			#LSDisc_General.printGreen("Step 0: Loading Gold Standard\n")
			#gold_standard = evaluation.load_gold_standard(input_dir + "_gt.cnl")

			output_dir = "output/" + str(dataset) + '/' + str(dataset)

			#result_file = output_dir + "_results.txt"

            #   create folder and file
			if not os.path.exists(os.path.dirname(output_dir)):
			    try:
			        os.makedirs(os.path.dirname(output_dir))
			    except OSError as exc: # Guard against race condition
			        if exc.errno != errno.EEXIST:
			            raise

			#f = open(result_file, "w")

			for i in range(iterations):

				LSDisc_General.printGreen("Step 1: Initializing MinHash LSH Index\n")
				LSDisc_MinHash.init_LSH(b, r)

				LSDisc_General.printGreen("Step 2: Do MAGIC!\n")
				LSDisc_MinHash.process_instances(i, input_dir, output_dir)
				
				groups = LSDisc_MinHash.dict_patterns_groups.values()

				print("Numgber of generated groups: %d\n" %len(groups))

				LSDisc_General.printGreen("Step 3: Removing duplicate groups\n")
				groups = LSDisc_General.remove_duplicate_buckets(groups)

				buckets = list()
				max_len = 0
				for g in groups:
					g = g[0]
					bucket = set()
					for p in g.getSetOfPatterns():
						bucket.add(p.getPatternNo())
					if bucket not in buckets:
						buckets.append(bucket)
						if len(bucket) > max_len:
							max_len = len(bucket)



				'''buckets = list()
				for set_of_groups in LSDisc_MinHash.dict_patterns_groups.values():
					for g in set_of_groups:
						bucket = set()
						for p in g.getSetOfPatterns():
							for e in p.getSetOfInstances():
								bucket.add(e)
						if bucket not in buckets:
							buckets.append(bucket)'''

				print("Max bucket len: %d" %max_len)

				print("Numgber of generated groups: %d\n" %len(buckets))

				

				#print("Numgber of generated groups: %d\n" %len(buckets))

				#print("Total instances contained in buckets = %d\n"%len(set.union(*buckets)))

				#if len(set.union(*buckets)) != len(set.union(*gold_standard.values())):
				#	LSDisc_General.printRed("ERROR: not all instances are contained in buckets!\n")

				'''precision, recall, f1 =  evaluation.evaluation_type_labels(buckets, gold_standard)

				f.write(str(precision) + "\t" + str(recall) + "\t" + str(f1) + "\n")
				f.write("--------------------------------------------------\n")'''

				'''for p_str in LSDisc_MinHash.dict_patterns.keys():
					for p in LSDisc_MinHash.dict_patterns[p_str]:
						print("%d\t: %s" %(p.getPatternNo(), str(p.getType())))'''

				#LSDisc_MinHash.clearDictionaries()

			#f.close()

			'''LSDisc_General.printGreen("Computing average scores\n")
			average_precision, average_recall, average_f1 = LSDisc_General.calculate_average_score(result_file)

			print(tabulate([	[str(average_precision), str(average_recall), str(average_f1)]],
												["Precision", "Recall", "f1_score"]))
			print "\n"'''

			#	write buckets to file
			LSDisc_General.output_buckets(output_dir, buckets)

			#	write patterns and corresponding instances to file
			LSDisc_General.output_patterns_instances(output_dir, LSDisc_MinHash.dict_patterns)


		elif args.hash_function[0] == "rp":
			
			LSDisc_General.printGreen("Step 1: Loading dataset " + dataset.split('/')[-1] + "\n")
			returned_values = LSDisc_RandomProjection.load_dataset_RP(dataset)
			if isinstance(returned_values, dict):
				dict_instances_properties = returned_values
			else:
				dict_instances_properties = returned_values[0]
				g = returned_values[1]

			LSDisc_General.printGreen("Step 2: Initializing Random Projection LSH Index\n")
			lsh_index = LSDisc_RandomProjection.init_LSH_RP(len(set.union(*dict_instances_properties.values())))

			LSDisc_General.printGreen("Step 3: Adding instances to Index\n")
			dict_instance_binary_list = LSDisc_RandomProjection.add_instances_to_index_RP(dict_instances_properties, lsh_index)

			LSDisc_General.printGreen("Step 4: Generating buckets\n")
			buckets = LSDisc_RandomProjection.create_buckets_RP(dict_instance_binary_list, lsh_index)
				#	write buckets to file
			LSDisc_General.output_buckets(dataset, buckets)	

	else:
		
		LSDisc_General.printGreen("Step 1: Initializing LSH Index\n")
		lsh_index = LSDisc_MinHash.init_LSH()

		#	load dataset in graph
		LSDisc_General.printGreen("Step 2: Loading dataset " + dataset.split('/')[-1] + "\n")
		returned_values = LSDisc_Incremental.load_dataset_Incr(dataset)
		if isinstance(returned_values, dict):
			dict_instances_properties = returned_values
		else:
			instances = returned_values[0]
			g = returned_values[1]

		LSDisc_General.printGreen("Step 3: Splitting dataset to training and test sets\n")
		train_set, test_set = LSDisc_Incremental.split_dataset(instances)

		LSDisc_General.printGreen("Step 4: Adding instances of training set to LSH\n")
		buckets, dict_instances_buckets =  LSDisc_Incremental.add_training_set(train_set, g, lsh_index, dataset)

		LSDisc_General.printGreen("Step 5: Adding instances of test set\n")
		buckets = LSDisc_Incremental.add_test_set(test_set, g, buckets, dict_instances_buckets, lsh_index, dataset)

		LSDisc_General.printGreen("Step 6: Removing duplicate buckets\n")
		buckets = LSDisc_General.remove_duplicate_buckets(buckets)
		print("\tBuckets generated : %d" %len(buckets))

		#	write buckets to file
		LSDisc_General.output_buckets(dataset,  buckets)

		LSDisc_General.output_test_set(dataset, test_set)


	if args.ground_truth:
		LSDisc_General.printGreen("Generating gold standard.\n")
		LSDisc_General.create_gold_standard(g, dataset)

	#LSDisc_General.printGreen("Step 4: Creating type profiles\n")
	#LSDisc_MinHash.create_type_profiles(buckets)
