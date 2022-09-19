To start the execution on the namenode:

``` 
hadoop jar reviews-bloom-filters-1.0-SNAPSHOT.jar it.unipi.moviesBloomFilters.Main data.tsv out 700000 0.0001 1
``` 

where:
- `data.tsv` is the path of the input file on hdfs
- `out` is the name of the output directory
- `700000` is the number of lines of each split
- `0.0001` is the P value
- `1` means that Job2 should be executed with the inmapperCombiner, if `2` Mapper that emits indexes, if `3` Mapper that emits full Bloom filters


To delete the old output files in hdfs:
``` 
hadoop fs -rm -R out && hadoop fs -rm -R out_2 && hadoop fs -rm -R out_3
``` 