To start the execution of Spark:

``` 
spark-submit --master yarn main.py data.tsv aggregate_by_key false 0 --iterative-tests
``` 

``` 
scp hadoop@172.16.4.188:~/spark_results.csv  spark_results.csv
``` 

``` 
scp main.py hadoop@172.16.4.188:~
``` 