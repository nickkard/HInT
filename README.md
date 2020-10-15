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
  python main.py 
  
### Evaluation

    python evalutaion.py <output_file.cnl> <gold_standard_file>.cnl
  
