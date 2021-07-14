# Spark SQL

Spark SQL is a spark module for structured data processing.

### SQL

One use of Spark SQL is to use SQL queries.

### Datasets

A dataset is a distributed collection of data. Datasets can be created using JVM Objects and can be manupulated using <strong>functional transformations</strong> (map(), flatmap()).

### Dataframe

A Dataframe is a dataset organised into named columns. It is conceptually equal to tables in relational database.

## Spark Session

The entry point to all the functionality in spark is the ```SparkSession``` class.

### Creating a basic SparkSession

```python
from pyspark.sql import SparkSession

spark = SparkSession
                .builder
                .setAppName("Spark Session creation")
                .config("some.spark configuration")
                .getOrCreate()
```