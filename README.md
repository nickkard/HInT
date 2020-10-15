#	HInT : Hybrid and Incremental Type Discovery for Large RDF Data Sources

## Source code

The source code is available inside *src* folder.


## Datasets

The datasets used in the evaluation of HInT are:

- BNF
- Conference
- DBpedia
- histmunic
- LUBM 2GB
- LUBM 8GB

The datasets are available at :	
[http://islcatalog.ics.forth.gr/dataset/hint](http://islcatalog.ics.forth.gr/dataset/hint)


## Requirements

- Python 2.7 or above
- [Datasketch](https://github.com/ekzhu/datasketch)


## Usage

### Type Discovery

To perform type discovery for a given *dataset*,*HInT* requires two files as input:

- *dataset*_instances_properties.txt where each line corresponds to an instance has the following format: 
    instance \t property set
- *dataset*_instances_types.txt where each line corresponds to an instance has the following format: 
    instance \t type set

The two input files should be in a directory named *dataset*.
To perform type discovery:
    
    python main.py <path_to_dataset_directory>
    
*HInT* produces two files in the directory *output/dataset/*:

- *dataset*.cnl which contains the generated buckets. Each line corresponds to a single bucket and contains the iderfier of the patterns contained in the bucket.
- *dataset*_patterns.cnl which has the following format:
   pattern_identifier \t instance set
   
  
### Evaluation
    python evalutaion.py <output_file.cnl> <gold_standard_file>.cnl
  
