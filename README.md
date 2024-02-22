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

In the [full project documentation](/documentation.pdf) you will find more details and the pseudo code of the three jobs.

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


