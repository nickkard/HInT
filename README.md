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

The first four are included in tthe *Datasets* folder of his repository. 
Four files are assosiated to each dataset:

- *dataset*.rdf/nt
- *dataset* _gt.cnl, which contains the ground truth
- *dataset* _instances_types.txt,  where each line corresponds to an instance and has the following format: 
   * instance \t type1 type2 ... typeN
- *dataset* _instances_properties.txt, where each line corresponds to an instance and has the following format: 
   * instance \t property1 property2 ... propertyN  

All six datasets are available at:	
[http://islcatalog.ics.forth.gr/dataset/hint](http://islcatalog.ics.forth.gr/dataset/hint)


## Requirements

- Python 2.7 or above
- [Datasketch](https://github.com/ekzhu/datasketch)
- Exectime (included in *src* folder)


## Usage

### Type Discovery

To perform type discovery for a given *dataset*:
    
    python main.py <path_to_dataset_directory>
    
E.g. to execute *HInT* on BNF subset:

    python main.py ../Datasets/BNF_subset/BNF_subset.rdf
    
To perform type discovery for a given *dataset* and measure the execution time:

    ./exectime python main.py <path_to_dataset_directory>

*HInT* produces two files in the directory *output/dataset/*:

- *dataset*.cnl, which contains the generated groups. Each line corresponds to a single group and contains the identifier of the patterns contained in the group. It has the following format :
  * identifier1 identifier2 ... identifierN
  
- *dataset*_patterns.cnl, which has the following format:
  * pattern_identifier \t instance1 instance2 ... instanceN
   
  
### Evaluation

To evaluate the generated groups against the gold standard:

    python evalutaion.py <output/dataset/dataset.cnl> <dataset_gt>.cnl
  
