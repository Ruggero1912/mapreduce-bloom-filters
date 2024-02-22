# Bloom Filters MapReduce implementation in Hadoop and Spark

## Introduction

This project investigates how to build Bloom Filters using the
MapReduce approach in Hadoop and Spark. The dataset used contains a list of movies
and their relative ratings. Ratings can be rounded to their closest integer values, obtaining
numbers from 1 to 10. This work aims to design and implement a MapReduce algorithm
for the construction of 10 Bloom Filters, one for each rating value, using both Hadoop and
Spark frameworks. The performances of the implementations are tested by computing
the false positive rate for each rating and by registering the execution times and resources
needed for each implementation.

### What is a Bloom Filter

A Bloom Filter is a space-efficient probabilistic data structure that is used to test whether
an element is a member of a set. In our case it’s used to check whether a movie has a
certain rounded rating, the bloom filter relative to that rounded rating will answer True
or False. False negatives are not possible for how Bloom filter construction is defined,
only false positives.

Refer to the [full project documentation](/documentation.pdf) for further details on Bloom Filters parameters and calculations.

### Dataset

The dataset used in this work contains 1256807 movies and their average ratings. It was
extracted on 30/08/2022 from [IMDb datasets](https://datasets.imdbws.com/).

![Frequency_ratings](https://github.com/Ruggero1912/movies-Bloom-filters/assets/63967908/29da0157-98c5-44a6-9106-59608966da56)

As we can see the dataset is not balanced, movies with a rounded rating equals to 6,7,8 make up over 75% of the entire dataset, being much more frequent than movies of other
ratings.

### Our objective

Our aim is to determine the best compromise between performances (memory occupation
and execution times) and False Positive Rate over the given dataset.

## Static analysis on memory occupation

In the following plot are reported the calculated `m` values for each rating class (according
to its numerosity) for some remarkable values of `P`:

![m-value-for-each-rating-onP-value](https://github.com/Ruggero1912/movies-Bloom-filters/assets/63967908/e4b72403-307a-49d9-874f-63df829b53db)

We can observe that the memory occupation caused by the minority classes (like the rate
1 or 2) are very low, while the most part of memory is occupied by the filters of the more
frequent ratings.
We can also observe that the memory occupation increases linearly with $−log_{10}(P)$.

For the impact of `P` on execution times we have to analyze the results obtained by the executions of the implementations.

## Implementations

We divided our solution in three main phases, that corresponds to three different jobs:

- Job 1: films per rating count,
- Job 2: Bloom Filters creation,
- Job 3: Bloom filters test.

> In the [full project documentation](/documentation.pdf) you will find more details and the pseudo code of the three jobs.

We implemented the Bloom filters both in Hadoop and in Spark.

### Hadoop implementation

The job 1 was implemented in Hadoop in two ways: the first one exploits a classical MapReduce approach, the
second one uses In-Mapper Combiner design pattern.

The job 2 instead was implemented in Hadoop in three ways:  the first and second exploits a classical MapReduce approach, the third uses
instead the In-Mapper Combiner design pattern.

The second implementation of job 2 is similar to the classical one, but this time the map function returns
a `Bitposition` variable, that contains the indexes of the bits that were set inside the Bloom Filter.
This implementation was made because calculating the values of `m` and `k` from
`P` and `n` with the optimized formulae, we have that `k` tends to be `≪ m`, and from that
we have a significant reduction in the amount of data to be transferred from the Map
phase.

Job 3 was implemented in two ways similarly to job 1.

> In the [full project documentation](/documentation.pdf) you will find more details on the implementation and schemas of the data flow for the implementations.

#### Execution results

In the following graph, we have a comparison between execution times of the different
implementations varying the p value. We can easily understand that implementations with
Optimized Mapper and In-Mapper Combiner outperform the first implementation of the
Mapper (without Bitposition), and also that the latter significantly increases execution
time when `P` becomes lower.

![Hadoop Job2 Execution Time Analysis](https://github.com/Ruggero1912/movies-Bloom-filters/assets/63967908/f8bb4c3d-f6a6-4bf9-a84e-bca77d493303)

For what concerns Physical Memory usage, the solution with In-Mapper Combiner is
preferred as it needs less memory for execution, probably because it instantiates only
one Bloom Filter object per rating for each Mapper instead of many. In general we can
appreciate an increasing memory usage when p decreases in all implementation, with a
significant rise in the Mapper base solution when p is particularly low.

> In the [full project documentation](/documentation.pdf) you will find more details on the results and additional plots

### Spark implementation

In order to implement Bloom filters in Spark we adopted some tuning over the Spark instance, like RDD repartitioning and we exploited broadcast variables to facilitate the implementation of the jobs.

> In the [full project documentation](/documentation.pdf) you will find more details on the optimizations done


We propose multiple implementations of job 2, in particular:
- solution 1: ”Classic Map Reduce” approach
- solution 2: ”GroupByKey” approach
- solution 3: ”AggregateByKey” approach

Here it is reported the execution time of job 2 for the three different implementations at various values of `P`:

![execution-time-spark-on-P-value](https://github.com/Ruggero1912/movies-Bloom-filters/assets/63967908/8bce739c-4391-488a-a8d8-6159b50178af)

> In the [full project documentation](/documentation.pdf) you will find the explaination of the implementations, with its PROs and CONs.
>
> You will find there also more tests on different configurations for the number of executors and core per executor parameters.

## Conclusions

It is important to note, however, that all tests were performed using a small dataset
(about 20Mb). This makes it difficult to appreciate the real differences between the
various proposed implementations and how they can scale. Although, among the various solutions, we were able to identify those that performed best in terms of space and
execution time.

#### Hadoop implementations performances at fixed P

We can see a big difference in terms of execution time between the
MapReduce classic approach and the other two implementations which have approximately the same
execution time. 

This behavior seems to be obvious because the classic version
of MapReduce will generate high intermediate data traffic (one bloom filter object per
record). 

Despite the presence of the combiner, before the data produced by the mapper
can be locally aggregated, there will be an high number of writes. When there is not
enough memory, **spilling to disk** may occur which significantly slows the execution of
the job.

Regarding the differences in terms of memory, a consequence of what was just said is that
in the classic map reduce approach we will obviously need more physical memory in order
to store a bloom filter per record, whereas the in-memory mapper combiner approach is
the best one since each mapper instantiates 10 bloom filters per input splits, saving a lot
of memory.

> Refer to the [full project documentation](/documentation.pdf) for the analysis on performance variations varying P for the Hadoop implementation

#### Spark implementation performances

The classic MapReduce approach is in general slower than the others, because it creates
a new Bloom Filter for every movieID and the huge amount of data generated from these
operations can cause swaps from disk to the main memory slowing down the application.

Considering the GroupByKey and AggregateByKey solutions, we can say that both have
similar execution performances even if the first has a slightly lower execution time on our
dataset.

We can notice that the first one is *less scalable*, in fact in this implementation some
executors could receive more data to process than others, but using our dataset we don’t
notice any important decrease in performances.

The second one, on the other hand, distributes better the work load among the executors,
but it also introduce some overhead to aggregate data from different executors, in fact,
using our dataset, it is slightly less efficient than the GroupByKey solution.

> Refer to the [full project documentation](/documentation.pdf) for the analysis on performance variations of the Spark implementation with respect to the number of executors

#### P values considerations

![image](https://github.com/Ruggero1912/movies-Bloom-filters/assets/63967908/b103eba2-6856-4852-92cb-8300fdf97075)

> Refer to the [full project documentation](/documentation.pdf) for the definition of *MultiPositive Rate*,
> for more analysis on the results varying P and for the final choice of P value.


