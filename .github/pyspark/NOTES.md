# Spark

```
P.S. : The below reference is in reference to pySpark (python + spark).
```

- Spark is nothing but a ```processing engine```.
- Spark can perform ```BATCH processing``` as well as ```Stream processing```
- Spark is not an extension of Hadoop but it uses hadoop as storage system.
- Spark has in-memory cluster computation capabilities to increase processing speed.
- Provides high level APIs for Java, Scala, Python.
- Spark is faster than Hadoop's in-memory mode computation.

## Batch Processing

Batch processing refers to processing of previously collected job in single batch.

## Stream Processing 

Deals with spark streaming data

# The key difference between Hadoop MapReduce and Spark

In fact, the key difference between Hadoop MapReduce and Spark lies in the approach to processing: Spark can do it in-memory, while Hadoop MapReduce has to read from and write to a disk. As a result, the speed of processing differs significantly – Spark may be up to 100 times faster. However, the volume of data processed also differs: Hadoop MapReduce is able to work with far larger data sets than Spark.

Now, let’s take a closer look at the tasks each framework is good for.

## Tasks Hadoop MapReduce is good for:

- Linear processing of huge data sets. Hadoop MapReduce allows parallel processing of huge amounts of data. It breaks a large chunk into smaller ones to be processed separately on different data nodes and automatically gathers the results across the multiple nodes to return a single result. In case the resulting dataset is larger than available RAM, Hadoop MapReduce may outperform Spark.
- Economical solution, if no immediate results are expected. Our Hadoop team considers MapReduce a good solution if the speed of processing is not critical. For instance, if data processing can be done during night hours, it makes sense to consider using Hadoop MapReduce.

## Tasks Spark is good for:

- Fast data processing. In-memory processing makes Spark faster than Hadoop MapReduce – up to 100 times for data in RAM and up to 10 times for data in storage.
- Iterative processing. If the task is to process data again and again – Spark defeats Hadoop MapReduce. Spark’s Resilient Distributed Datasets (RDDs) enable multiple map operations in memory, while Hadoop MapReduce has to write interim results to a disk.
- Near real-time processing. If a business needs immediate insights, then they should opt for Spark and its in-memory processing.
- Graph processing. Spark’s computational model is good for iterative computations that are typical in graph processing. And Apache Spark has GraphX – an API for graph computation.
- Machine learning. Spark has MLlib – a built-in machine learning library, while Hadoop needs a third-party to provide it. MLlib has out-of-the-box algorithms that also run in memory. But if required, our Spark specialists will tune and adjust them to tailor to your needs.
- Joining datasets. Due to its speed, Spark can create all combinations faster, though Hadoop may be better if joining of very large data sets that requires a lot of shuffling and sorting is needed.

# Resilient Distributed Dataset (RDD):

[<img src="https://databricks.com/wp-content/uploads/2018/05/rdd.png" width="100%" />](https://databricks.com/wp-content/uploads/2018/05/rdd.png)


At the core, an RDD is an immutable distributed collection of elements of your data, partitioned across nodes in your cluster that can be operated in ```parallel``` with a low-level API that offers transformations and actions.

Spark revolves around the concept of a resilient distributed dataset (RDD), which is a fault-tolerant collection of elements that can be operated on in ```parallel```. 

There are two ways to create RDDs: ```parallelizing``` an existing collection in your driver program, or referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.

### Spark Context

The first thing a Spark program must do is to create a SparkContext object, which tells Spark how to access a cluster. To create a SparkContext you first need to build a SparkConf object that contains information about your application.

### Linking with Spark

```python
from pyspark import SparkContext, SparkConf
```

### SparkConf

Configuration for a Spark application. Used to set various Spark parameters as key-value pairs.

Most of the time, you would create a SparkConf object with new SparkConf(), which will load values from any spark.* Java system properties set in your application as well. In this case, parameters you set directly on the SparkConf object take priority over system properties. [Refer Here](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/SparkConf.html)

```python
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
```

# Parallelized Collections:

* In order to run our collection in parallel, we need to parallelize them using the below method.

Parallelized collections are created by calling SparkContext’s parallelize method on an existing iterable or collection in your driver program. The elements of the collection are copied to form a distributed dataset that can be operated on in parallel. 

For example, here is how to create a parallelized collection holding the numbers 1 to 5:

```python
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
```
- ```sc``` here represents SparkContext.

# RDD Operations

RDDs support two types of operations
- <strong>Transformation</strong>: Creats new dataset from a new one. 

For Example:
<strong>map</strong> is a transformation that passes each dataset element through a function and return new RDD representing the result.

- <strong>Action</strong>: Returns a value to the driver code, after running a computation on the dataset

For Example:
<strong>reduce</strong> is an action that aggregates all the elements of the RDD using some function and returns the final result to the driver program.

# All the tranformations are lazy!!

All transformations in Spark are lazy, in that they do not compute their results right away. Instead, they just remember the transformations applied to some base dataset (e.g. a file). The transformations are only computed when an action requires a result to be returned to the driver program. This design enables Spark to run more efficiently. For example, we can realize that a dataset created through map will be used in a reduce and return only the result of the reduce to the driver, rather than the larger mapped dataset.

## Persisting an RDD in memory

By default, each transformed RDD may be recomputed each time you run an action on it. However, you may also persist an RDD in memory using the persist (or cache) method, in which case Spark will keep the elements around on the cluster for much faster access the next time you query it. There is also support for persisting RDDs on disk, or replicated across multiple nodes.

# Basics

Consider the below program:

```python
lines = sc.textFile("data.txt")
lineLengths = lines.map(lambda s: len(s)) # passes each line from the file to map() and stores a list of lengths of the lines
totalLength = lineLengths.reduce(lambda a, b: a + b)
```

- The first line defines a base RDD from an external file. This dataset is not loaded in memory or otherwise acted on: lines is merely a pointer to the file. 
- The second line defines lineLengths as the result of a map transformation. Again, lineLengths is not immediately computed, due to laziness. 
- Finally, we run reduce, which is an action. At this point Spark breaks the computation into tasks to run on separate machines, and each machine runs both its part of the map and a local reduction, returning only its answer to the driver program.

If we also wanted to use lineLengths again later, we could add:

```python
lineLengths.persist()
```

# Passing Functions to Spark

Spark’s API relies heavily on passing functions in the driver program to run on the cluster. There are three recommended ways to do this:

- <strong>Lambda expressions</strong>, for simple functions that can be written as an expression. (Lambdas do not support multi-statement functions or statements that do not return a value.)
- <strong>Local defs</strong> inside the function calling into Spark, for longer code.
- <strong>Top-level functions</strong> in a module.

For example, to pass a longer function than can be supported using a lambda, consider the code below:

```python
if __name__ == "__main__":
    def myFunc(s):
        words = s.split(" ")
        return len(words)

    sc = SparkContext(...)
    sc.textFile("file.txt").map(myFunc)

```

Note that while it is also possible to pass a reference to a method in a class instance (as opposed to a singleton object), this requires sending the object that contains that class along with the method. For example, consider:

```python
class MyClass(object):
    def func(self, s):
        return s
    def doStuff(self, rdd):
        return rdd.map(self.func)
```
Here, if we create a new MyClass and call doStuff on it, the map inside there references the func method of that MyClass instance, so the whole object needs to be sent to the cluster.

In a similar way, accessing fields of the outer object will reference the whole object:

```python
class MyClass(object):
    def __init__(self):
        self.field = "Hello"
    def doStuff(self, rdd):
        return rdd.map(lambda s: self.field + s)
```

To avoid this issue, the simplest way is to copy field into a local variable instead of accessing it externally:

```python
def doStuff(self, rdd):
    field = self.field
    return rdd.map(lambda s: field + s)
```

# Closures

Consider the naive RDD element sum below, which may behave differently depending on whether execution is happening within the same JVM. A common example of this is when running Spark in local mode (--master = local[n]) versus deploying a Spark application to a cluster (e.g. via spark-submit to YARN):

```python
counter = 0
rdd = sc.parallelize(data)

# Wrong: Don't do this!!
def increment_counter(x):
    global counter
    counter += x
rdd.foreach(increment_counter)

print("Counter value: ", counter)
```

## Local vs. cluster modes

The above code may not work as intended

- To execute a job, spark breaks the processing of job into tasks, each of the task is then executed by an executor.
- Before the execution Spark calculates the ```closure```

```
Closures are those variables and functions that must be visible to the executor to perform the computation on the RDD, in the current case ```foreach()```.
```

- This closure is serialized and sent to the executors.
- The variables in the closures sent to the executors are now copies, so now when foreach() uses the counter variable it is not longer the variable present in the driver node.

- Each executor has its own copy of the variable which is NOT THE SAME VARIABLE AS IN THE DRIVER NODE.

# Accumulators

Accumulators in Spark are used to provide a mechanism for safely updating a variable when execution is split up across worker nodes in a cluster

```
In general, closures - constructs like loops or locally defined methods, should not be used to mutate some global state.
```

# Transformations

[Refer this document](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations)

# Actions

[Refer this document](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions)