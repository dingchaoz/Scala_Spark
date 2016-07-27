// Databricks notebook source exported at Tue, 26 Jul 2016 03:26:47 UTC
// MAGIC %md #SparkSQL and DataFrames
// MAGIC 
// MAGIC * Some form of SQL processing has been part of the core distribution since 1.0 (April 2014)
// MAGIC   * The purpose is to runs SQL / HiveQL queries, optionally alongside or replacing existing Hive deployments
// MAGIC 
// MAGIC * __Modern SparkSQL and DataFrames represent the same thing: a new language-independent, high-performance query engine implementation__
// MAGIC   * SparkSQL/DataFrames is different from, and replaces, a variety of older approaches including Shark, Hive-on-Spark, and SchemaRDD
// MAGIC   * Although Spark contains its own data processing engine, it can integrate closely with a Hive metastore
// MAGIC 
// MAGIC <img src="http://i.imgur.com/kOZqeNo.png" width="600">

// COMMAND ----------

// MAGIC %md ##SparkSQL, DataFrames and DataSets Represent the Same Component in Modern Spark
// MAGIC 
// MAGIC ##### This component allows major optimizations by Spark, far beyond what is possible with the RDD API, in terms of both execution speed and data storage size.
// MAGIC 
// MAGIC * DataFrame/DataSet API and SQL (typically via the HiveQL parser) are equivalent ways to do the same tasks.
// MAGIC   * SQL serves many general purposes, including analytic work via BI tools (over JDBC and the Thriftserver)
// MAGIC   * DataFrames offer more programmatic control and an API familiar to users of Pandas or R
// MAGIC   * DataSets are a generalization of DataFrames and the underlying engine, to support more data types and stronger type enforcement

// COMMAND ----------

// MAGIC %md ##DataFrame API
// MAGIC 
// MAGIC * Enable wider audiences beyond ?Big Data? engineers to leverage the power of distributed processing
// MAGIC   * Seamless integration with all big data tooling and infrastructure via Spark
// MAGIC   * Designed from the ground-up to support modern big data and data science applications
// MAGIC 
// MAGIC * Inspired by data frames in R and Python (Pandas), but offering transparent scale-out support
// MAGIC   * ... from kilobytes of data on a single laptop to petabytes on a large cluster
// MAGIC 
// MAGIC * Support for a wide array of data formats and storage systems
// MAGIC * State-of-the-art optimization and code generation through the Spark SQL Catalyst optimizer
// MAGIC * APIs for Python, Java, Scala, and R
// MAGIC 
// MAGIC See
// MAGIC * https://spark.apache.org/docs/latest/sql-programming-guide.html 
// MAGIC * http://databricks.com/blog/2015/02/17/introducing-dataframes-in-spark-for-large-scale-data-science.html

// COMMAND ----------

// MAGIC %md ##DataFrames
// MAGIC The preferred abstraction in Spark (introduced in 1.3)
// MAGIC * Strongly typed collection of distributed elements
// MAGIC   * Built on Resilient Distributed Datasets
// MAGIC * _Immutable once constructed_
// MAGIC * Track lineage information to efficiently recompute lost data
// MAGIC * Enable operations on collection of elements in parallel
// MAGIC 
// MAGIC You construct DataFrames
// MAGIC * by parallelizing existing collections (e.g., Pandas DataFrames) 
// MAGIC * by transforming an existing DataFrame
// MAGIC * from files in HDFS, Hive tables, or any other storage system (e.g., Parquet in S3)

// COMMAND ----------

// MAGIC %md ##Why Use DataFrames instead of RDDs?
// MAGIC 
// MAGIC 
// MAGIC * For new users familiar with data frames in other programming languages, this API should make them feel at home
// MAGIC * For existing Spark users, the API will make Spark easier to program than using RDDs
// MAGIC * For both sets of users, DataFrames will improve performance through intelligent optimizations and code-generation
// MAGIC <br/>
// MAGIC <br/>
// MAGIC <br/>
// MAGIC <img src="http://i.imgur.com/0uLWgHl.png" width="600">

// COMMAND ----------

// MAGIC %md ##DataFrames can be significantly faster than RDDs. 
// MAGIC 
// MAGIC And they perform the same, regardless of language.
// MAGIC 
// MAGIC <img src="http://i.imgur.com/CQCBB2E.png" width="600">

// COMMAND ----------

// MAGIC %md ## Write Less Code: Input & Output
// MAGIC 
// MAGIC Unified interface to reading/writing data in a variety of formats.
// MAGIC 
// MAGIC ```
// MAGIC val df = sqlContext.
// MAGIC   read.
// MAGIC   format("json").
// MAGIC   option("samplingRatio", "0.1").
// MAGIC   load("/Users/spark/data/stuff.json")
// MAGIC 
// MAGIC df.write.
// MAGIC    format("parquet").
// MAGIC    mode("append").
// MAGIC    partitionBy("year").
// MAGIC    saveAsTable("faster-stuff")
// MAGIC ```

// COMMAND ----------

// MAGIC %md ##Data Sources supported by DataFrames
// MAGIC 
// MAGIC <img src="http://i.imgur.com/sdYuTIv.png" width="600">

// COMMAND ----------

// MAGIC %md ##Write Less Code: High-Level Operations
// MAGIC 
// MAGIC Solve common problems concisely with DataFrame functions:
// MAGIC * selecting columns and filtering
// MAGIC * joining different data sources
// MAGIC * aggregation (count, sum, average, etc.)
// MAGIC * plotting results (e.g., with Pandas)

// COMMAND ----------

// MAGIC %md ##Write Less Code: Compute an Average
// MAGIC 
// MAGIC <img src="http://i.imgur.com/gLDgzDP.png" width="600">
// MAGIC 
// MAGIC __The Spark code is much shorter ... but it's not clear. It's complex, and the programmer's intent is not communicated clearly.__
// MAGIC 
// MAGIC Let's look at making that both *shorter* __and__ *easier*. First, we'll set up some data to use:

// COMMAND ----------

// set up RDD
val data = sc.parallelize(List(("Jim", 30), ("Anne", 31), ("Jim", 32)))

// set up DataFrame
import org.apache.spark.sql.functions._
data.toDF("name", "age").registerTempTable("people")

// COMMAND ----------

// MAGIC %md Run the RDD-based Spark code to calculate average ages for each name:

// COMMAND ----------

data.map { x => (x._1, (x._2, 1)) }
  .reduceByKey { case (x,y) => 
      (x._1 + y._1, x._2 + y._2) }
  .map { x => (x._1, x._2._1 / x._2._2) }
  .collect()

// COMMAND ----------

// MAGIC %md Now the same calculation, using the DataFrame API.
// MAGIC 
// MAGIC You'll probably agree that the following is much easier to understand and harder to mess up when coding:

// COMMAND ----------

sqlContext.table("people")
          .groupBy("name")
          .agg(avg("age"))
          .collect()

// COMMAND ----------

// MAGIC %md ###... and Spark can optimize it!
// MAGIC 
// MAGIC The full API Docs are here:
// MAGIC 
// MAGIC * Scala - http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.package
// MAGIC * Java - http://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/package-summary.html
// MAGIC * Python - http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#module-pyspark.sql
// MAGIC * R - http://spark.apache.org/docs/latest/api/R/index.html
// MAGIC 
// MAGIC __BUT for a shortcut to finding APIs you need, learning, or trying to solve a problem, bookmark (or at least look first at) the following items:__
// MAGIC 
// MAGIC In org.apache.spark.sql:
// MAGIC 1. DataFrame class
// MAGIC 2. Column class
// MAGIC 3. functions object
// MAGIC 4. GroupedData class

// COMMAND ----------

// MAGIC %md ##Create a DataFrame
// MAGIC 
// MAGIC DataFrames need a schema: consistent columns, each with a name and a type.
// MAGIC 
// MAGIC We can create one from a Parquet file easily, because all of that schema info is built into the Parquet file:

// COMMAND ----------

val people = sqlContext.read.parquet("dbfs:/mnt/training/ssn/names.parquet")

// COMMAND ----------

// MAGIC %md ## Use DataFrames

// COMMAND ----------

people.show(5)

// COMMAND ----------

var popular = people.filter($"total" > 80000)
popular.show(5)

// COMMAND ----------

people.select($"firstName", $"total" * 1000)

// COMMAND ----------

// MAGIC %md Many DataFrame API functions have overloads that can take Column objects or SQL string column names (select) or even expressions (filter):

// COMMAND ----------

println(people.filter("total < 80000"))
println(people.select("firstName", "total"))

// COMMAND ----------

// MAGIC %md We'll learn more about columns and expressions in a bit. For now, here are a couple more examples of logic (grouping/counting, joining) using the DataFrame API:

// COMMAND ----------

people.groupBy("year").count

// COMMAND ----------

val scores = sc.parallelize(List( ("James", 100), ("Linda", 100) )).toDF("name", "score")

people.join(scores, people("firstName") === scores("name"), "inner")

// COMMAND ----------

// MAGIC %md ## Connecting DataFrames and SparkSQL

// COMMAND ----------

people.registerTempTable("people")
sqlContext.sql("SELECT count(*) FROM people")

// COMMAND ----------

// MAGIC %md Note that registerTempTable just creates a symbol representing the query (a bit like a SQL View) so that, later, we can use SQL to reference that query. We can treat it like a table, and the parser will understand what we mean. These symbols ("tables") are available to any code using the sqlContext -- they can also access this table directly if they like (without SQL) by calling ```sqlContext.table("people")```

// COMMAND ----------

// MAGIC %md ## DataFrames and SparkSQL
// MAGIC 
// MAGIC The DataFrames API provides a programmatic interface?really, a domain-specific language (DSL)?for interacting with your data.
// MAGIC * Spark SQL allows you to manipulate distributed data with SQL queries. Currently, two SQL dialects are supported.
// MAGIC   * If you?re using a Spark SQLContext, the only supported dialect is ?sql,? a rich subset of SQL 92.
// MAGIC   * If you?re using a HiveContext, the default dialect is "hiveql", corresponding to Hive's SQL dialect. ?sql? is also available, but ?hiveql? is a richer dialect.

// COMMAND ----------

// MAGIC %md ##SparkSQL
// MAGIC 
// MAGIC You issue SQL queries through a SQLContext or HiveContext, using the sql() method.
// MAGIC * The sql() method returns a DataFrame.
// MAGIC * You can mix DataFrame methods and SQL queries in the same code.
// MAGIC * To use SQL, you must either:
// MAGIC   * query a persisted Hive table, or
// MAGIC   * make a table alias for a DataFrame, using registerTempTable()

// COMMAND ----------

// MAGIC %md ##Transformations, Actions, Laziness
// MAGIC 
// MAGIC DataFrames are lazy. Transformations contribute to the query plan, but they don't execute anything. 
// MAGIC Actions cause the execution of the query.
// MAGIC 
// MAGIC |Transformations|Actions|
// MAGIC |---|---|
// MAGIC |filter,select,drop,intersect,join|count,collect,show,head,take
// MAGIC 
// MAGIC 
// MAGIC *Actions cause the execution of the query.*
// MAGIC 
// MAGIC __What, exactly, does execution of the query mean? It means:__
// MAGIC * Spark initiates a distributed read of the data source
// MAGIC * The data flows through the transformations (the RDDs resulting from the Catalyst query plan)
// MAGIC * The result of the action is pulled back into the driver JVM.

// COMMAND ----------

people.take(5)

// COMMAND ----------

// MAGIC %md ##DataFrames have Schemas
// MAGIC 
// MAGIC In the previous example, we created DataFrames from Parquet and JSON data.
// MAGIC * A Parquet table has a schema (column names and types) that Spark can use. Parquet also allows Spark to be efficient about how it pares down data.
// MAGIC * Spark can infer a Schema from a JSON file.

// COMMAND ----------

// MAGIC %md ##Columns
// MAGIC 
// MAGIC When we say column here, what do we mean?
// MAGIC 
// MAGIC * a DataFrame column is an abstraction. It provides a common column-oriented view of the underlying data, regardless of how the data is really organized.
// MAGIC 
// MAGIC * Columns are important because much of the DataFrame API consists of functions that take or return columns (even if they don?t look that way at first).

// COMMAND ----------

// MAGIC %md ## How Do Columns Map to Common Data Types?
// MAGIC 
// MAGIC <img src="http://i.imgur.com/QfUf7Ub.png" width="500">

// COMMAND ----------

// MAGIC %md #### SQL, CSV, JSON ...
// MAGIC 
// MAGIC <img src="http://i.imgur.com/ajxRmWN.png" width="720">

// COMMAND ----------

// MAGIC %md ##Accessing Colums
// MAGIC 
// MAGIC Assume we have a DataFrame, df, that reads a data source that has "first", "last", and "age" columns.
// MAGIC 
// MAGIC |Python|Scala|Java|R|
// MAGIC |---|---|---|---|
// MAGIC |df["first"], df.first?|df("first"), $"first"?|df.col("first")|df$first|
// MAGIC 
// MAGIC 
// MAGIC ? In Python, it?s possible to access a DataFrame?s columns either by attribute (df.age) or by indexing (df['age']). While the former is convenient for interactive data exploration, you should use the index form. It's future proof and won?t break with column names that are also attributes on the DataFrame class.
// MAGIC 
// MAGIC ?The $ syntax can be ambiguous if there are multiple DataFrames in the lineage.

// COMMAND ----------

// MAGIC %md ## printSchema()
// MAGIC 
// MAGIC You can have Spark tell you what it thinks the data schema is, by calling the printSchema() method.  (This is mostly useful in the shell.)

// COMMAND ----------

people.printSchema

// COMMAND ----------

// MAGIC %md ##Schema Inference
// MAGIC 
// MAGIC * Some data sources (e.g., parquet) can expose a formal schema; others (e.g., plain text files) don?t. How do we fix that?
// MAGIC * You can create an RDD of a particular type and let Spark infer the schema from that type. We?ll see how to do that in a moment.
// MAGIC * You can use the API to specify the schema programmatically.
// MAGIC 
// MAGIC The key thing to remember is that a schema has consistent columns, column types, and column names. We can "mix-and-match" where these come from, as long as all three are there. For example, a CSV file might provide consistent columns and types, and we might fill in the column names; alternatively, a file might have column data and fields names, but we need to supply the types.

// COMMAND ----------

// MAGIC %md ##Schema Application Example
// MAGIC 
// MAGIC Suppose you have a file that looks like this:
// MAGIC 
// MAGIC ```
// MAGIC Erin,Shannon,F,42
// MAGIC Norman,Lockwood,M,81
// MAGIC Miguel,Ruiz,M,64
// MAGIC Rosalita,Ramirez,F,14
// MAGIC Ally,Garcia,F,39
// MAGIC Claire,McBride,F,23
// MAGIC Abigail,Cottrell,F,75
// MAGIC José,Rivera,M,59
// MAGIC Ravi,Dasgupta,M,25
// MAGIC ...
// MAGIC ```
// MAGIC 
// MAGIC The file has no schema, but it?s obvious there is one:
// MAGIC 
// MAGIC |Field|Type|
// MAGIC |---|---|
// MAGIC |First name|string|
// MAGIC |Last name|string|
// MAGIC |Gender|string|
// MAGIC |Age|integer|
// MAGIC 
// MAGIC Let's see how to get Spark to infer that schema.

// COMMAND ----------

case class Person(firstName: String,
                  lastName:  String,
                  gender:    String,
                  age:       Int)

val rdd = sc.textFile("people.csv")

val peopleRDD = rdd.map { line =>
  val cols = line.split(",")
  Person(cols(0), cols(1), cols(2), cols(3).toInt)
}

val df = peopleRDD.toDF

// df: DataFrame = [firstName: string, lastName: string, gender: string, age: int]

// COMMAND ----------

// MAGIC %md ##Schema Inference
// MAGIC 
// MAGIC We can also force schema inference
// MAGIC * ... without creating our own People type, 
// MAGIC * by using a fixed-length data structure (such as a tuple) 
// MAGIC * and supplying the column names to the toDF() method.

// COMMAND ----------

// MAGIC %md ##Why do you have to use a tuple?
// MAGIC * In Python, you don?t. You can use any iterable data structure (e.g., a list).
// MAGIC * In Scala, you do. Tuples have fixed lengths and fixed types for each element at compile time. For instance:
// MAGIC 
// MAGIC   __```Tuple4[String,String,String,Int]```__
// MAGIC 
// MAGIC * The DataFrames API uses this information to infer the number of columns and their types. It cannot do that with an array.

// COMMAND ----------

val rdd = sc.textFile("people.csv")

val peopleRDD = rdd.map { line =>
  val cols = line.split(",")
  (cols(0), cols(1), cols(2), cols(3).toInt)
}

val df = peopleRDD.toDF("firstName", "lastName", "gender", "age")

// COMMAND ----------

// MAGIC %md ##One last method for schemas
// MAGIC 
// MAGIC Internally, a Dataframe schema is represented by the public StructType type: http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.types.StructType
// MAGIC 
// MAGIC You can use this type to create a schema as in the following example.

// COMMAND ----------

import org.apache.spark.sql.types._

val schema = StructType(Seq( StructField("name", StringType, false), StructField("age", IntegerType, false) ))

// COMMAND ----------

// MAGIC %md __But__ ... when and where would that be useful?
// MAGIC 
// MAGIC Mainly when loading files with lots of fields ... case classes and tuples are limited to 22 elements, and lots of real-world tables are hundreds of columns wide.
// MAGIC 
// MAGIC In addition, it's easy to build StructType schemas programmatically from a source of schema information -- for example, lots of legacy systems contain files that describe column names and types, so we can create a Spark Dataframe schema by reading that file and building a StructType to apply to corresponding raw data files.

// COMMAND ----------

// MAGIC %md ##show()
// MAGIC 
// MAGIC You can look at the first n elements in a DataFrame with the show() method. If not specified, n defaults to 20.
// MAGIC 
// MAGIC This method is an action. It:
// MAGIC * reads (or re-reads) the input source
// MAGIC * executes the RDD DAG across the cluster
// MAGIC * pulls the n elements back to the driver JVM
// MAGIC * displays those elements in a tabular form

// COMMAND ----------

people.show(5)

// COMMAND ----------

// MAGIC %md ##select()
// MAGIC 
// MAGIC select() is like a SQL SELECT, allowing you to limit the results to specific columns.

// COMMAND ----------

people.select($"firstName", $"year").show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC select() also lets you create on-the-fly *derived* columns (similar to SQL select)

// COMMAND ----------

people.select($"firstName", $"year", $"year" > 1950, $"year" + 1000).show(5)

// COMMAND ----------

// MAGIC %md Note that the expressions like ```$"year" + 1000``` are also Columns (and that's what you'll see in the DataFrame.select method signature).
// MAGIC 
// MAGIC The operators here (> or +) are just methods of Column, and they also return a Column:

// COMMAND ----------

people("year") + 10

// COMMAND ----------

(people("year") + 10) == (people("year").+(10))

// COMMAND ----------

people.apply("year").+(10)

// COMMAND ----------

// MAGIC %md ... and of course you can use SQL:

// COMMAND ----------

people.registerTempTable("people")
sqlContext.sql("SELECT firstName, year, year + 1000 as future FROM people").show(5)

// COMMAND ----------

// MAGIC %md ##filter()
// MAGIC 
// MAGIC The filter() method allows you to filter rows out of your results.

// COMMAND ----------

people.filter($"year" > 1990).select($"firstName", $"year").show(5)

// COMMAND ----------

// MAGIC %md Here's how filter appears (as a WHERE clause) in SQL:

// COMMAND ----------

sqlContext.sql("SELECT firstName, year FROM people WHERE year > 1990").show(5)

// COMMAND ----------

// MAGIC %md ##orderBy()
// MAGIC 
// MAGIC The orderBy() method allows you to sort the results.

// COMMAND ----------

people.filter(people("year") > 1990).select(people("firstName"), people("year")).orderBy(people("year"), people("firstName")).show(5)

// COMMAND ----------

// MAGIC %md It?s easy to reverse the sort order: look for the __desc__ method calls in the following example.

// COMMAND ----------

people.filter(people("year") > 1990).select(people("firstName"), people("year")).orderBy(people("year") desc, people("firstName") desc).show(5)

// COMMAND ----------

// MAGIC %md In SQL, it's pretty normal looking:

// COMMAND ----------

sqlContext.sql("SELECT firstName, year FROM people ORDER BY year DESC, firstName DESC").show(5)

// COMMAND ----------

// MAGIC %md ##as() or alias()
// MAGIC 
// MAGIC as() or alias() allows you to rename a column
// MAGIC 
// MAGIC * it?s especially useful with generated columnns (which are sometimes assigned names with non-alphanumeric characters)
// MAGIC * and joins (to disambiguate columns with same name in each of the joined tables)
// MAGIC 
// MAGIC note: in Python you must use "alias" because "as" is a reserved word

// COMMAND ----------

people.select($"firstName", $"year", ($"year" > 2000).as("recent")).show(5)

// COMMAND ----------

sqlContext.sql("SELECT firstName, year, year > 2000 AS recent FROM people").show(5)

// COMMAND ----------

// MAGIC %md ##groupBy()
// MAGIC 
// MAGIC groupBy() is used to group data (rows) by their value(s) (in one or more specified columns) for aggregations, such as count() or sum()

// COMMAND ----------

people.groupBy($"year").count.show(5)

// COMMAND ----------

import org.apache.spark.sql.functions._
people.groupBy($"year").agg(sum($"total")).show(5)

// COMMAND ----------

sqlContext.sql("SELECT year, count(*) AS count, SUM(total) AS total FROM people GROUP BY year").show(5)

// COMMAND ----------

// MAGIC %md ##Joins
// MAGIC 
// MAGIC Let's assume we had a file that looked like this:
// MAGIC 
// MAGIC ```
// MAGIC Dacia:Rosella:Samborski:F:1940-08-06:274357:932-39-7400
// MAGIC Loria:Suzie:Cassino:F:1964-01-31:166618:940-40-2137
// MAGIC Lashaunda:Markita:Rockhill:F:1936-06-02:185766:923-83-5563
// MAGIC Candace:Marcy:Goike:F:1971-09-25:92497:935-40-2967
// MAGIC Marhta:Filomena:Bonin:F:1926-06-29:40013:968-22-1158
// MAGIC Rachel:Gwyn:Mcmonigle:F:1951-04-27:211468:926-47-4803
// MAGIC Lorine:Valencia:Bilous:F:2012-09-08:26612:992-10-1262
// MAGIC Alene:Berniece:Somji:F:1926-04-25:74027:989-16-1381
// MAGIC Sadye:Mara:Morrisseau:F:1930-07-01:209278:971-50-8157
// MAGIC Shawn:Reginia:Battisti:F:1962-08-26:190167:993-42-5846
// MAGIC ```
// MAGIC 
// MAGIC Suppose we want to join this list against our list of names ... perhaps we'd like to estimate how unusual each first name is.

// COMMAND ----------

val sampleData = sqlContext.read.format("com.databricks.spark.csv")
  .option("delimiter", ":")
  .load("dbfs:/mnt/training/dataframes/people.txt").select($"C0" as "name", $"C4" as "birthdate")

sampleData.show(5)

// COMMAND ----------

// MAGIC %md We can join people to sampleData on the name field with syntax like this:

// COMMAND ----------

people.join(sampleData, people("firstName") === sampleData("name")).show(5)

// COMMAND ----------

// MAGIC %md We might be able to add some business value if we can join on the year as well as the name. We can use some built-in DataFrame column functions to extract the year from the (string) birthdate column, and try the join again:

// COMMAND ----------

// aside from the DataFrame and Column classes,
// this is where most of the helper functions live:
import org.apache.spark.sql.functions._ 
// split(col, pattern) is located here

// lets us use strongly-typed value IntegerType below:
import org.apache.spark.sql.types._ 

// cast is a method of the Column class, and withColumn is a method of the DataFrame class:
val sampleDataWithYear = sampleData.withColumn("birthyear", split($"birthdate", "-")(0) cast IntegerType)

sampleDataWithYear.show(5)

// COMMAND ----------

val joined = people.join(sampleDataWithYear, people("firstName") === sampleDataWithYear("name") && people("year") === sampleDataWithYear("birthyear"))

joined.select("firstName", "total", "year").show(5)

// COMMAND ----------

// MAGIC %md ##What Other Handy Functions Can I Use?
// MAGIC 
// MAGIC In addition to the members of DataFrame and Column, there are over 100 helpful functions that operate on columns, defined in ```org.apache.spark.sql.functions```
// MAGIC 
// MAGIC These include:
// MAGIC * Date/Time : Convert timestamps, extract fields, arithmetic on dates, parse arbitrary date strings
// MAGIC * Math : Factorial, log, radians, base change...
// MAGIC * Conditional : Greatest, least, isnull...
// MAGIC * String: Base64, regex, length, trim, split, levenshtein...
// MAGIC 
// MAGIC Here is an example for a common task: converting Unix timestamps into date formatted string, and then into SQL datetime types:

// COMMAND ----------

val data = Array((2,1420001316L), (4,1440000006L), (6,1410001316L)) 
val df = sc.parallelize(data).toDF("id", "timestamp")

val withStringDate = df.withColumn("string_date", from_unixtime(df("timestamp")))
val withRealDate = withStringDate.withColumn("real_date", to_utc_timestamp(withStringDate("string_date"), "GMT"))
withRealDate.show

// COMMAND ----------

// MAGIC %md Note the schema information about the columns in the above output.
// MAGIC 
// MAGIC But what if there is just no way to solve our problem with all of these APIs?

// COMMAND ----------

// MAGIC %md ##User-Defined Functions
// MAGIC 
// MAGIC Above, we extracted the year from the birthdate string by string-splitting and casting using built-in functions.
// MAGIC 
// MAGIC If we had an actual date or datetime column, we could also use the built-in year() function to solve the problem.
// MAGIC 
// MAGIC But suppose we didn't have either of those options, and we needed to do some custom processing on each row, to produce a new column value.
// MAGIC 
// MAGIC In this example, we'd like to apply a function like the following:
// MAGIC 
// MAGIC ```
// MAGIC def extractYear(birthdate:String) = birthdate.split("-")(0).toInt
// MAGIC extractYear("2009-10-31")
// MAGIC 
// MAGIC res42: Int = 2009
// MAGIC ```
// MAGIC 
// MAGIC We can create a function like that, and convert it to a Spark UDF that operates on (and returns a column):

// COMMAND ----------

val extractYear = sqlContext.udf.register("pullOutYear", (birthdate:String) => birthdate.split("-")(0).toInt)

sampleData.select($"name", $"birthdate", extractYear($"birthdate") as "birthyear").show(5)

// COMMAND ----------

// MAGIC %md What's that ```pullOutYear``` string doing in there? That name is available to SQL commands:

// COMMAND ----------

sampleData.registerTempTable("sampleData")
sqlContext.sql("SELECT name, birthdate, pullOutYear(birthdate) AS birthyear FROM sampleData").show(5)

// COMMAND ----------

// MAGIC %md __It is strongly preferable to use a combination of built-in functions, rather than defining your own.__
// MAGIC 
// MAGIC Why? Besides being (usually) easier, Spark *cannot optimizie your UDF functions*
// MAGIC 
// MAGIC In fact, Spark cannot "see inside" your UDF functions at all. This means, for example, that a join on a UDF will require a Cartesian join, then testing every pair of values against your UDF. 
// MAGIC 
// MAGIC In addition, most built-in functions include support for code generation, which offers significant speed improvements.
// MAGIC 
// MAGIC But if a UDF is the only reasonable solution, it is easy to add one.

// COMMAND ----------

// MAGIC %md ##SparkSQL: Just a Little More Info
// MAGIC 
// MAGIC As we've seen, Spark SQL operations generally return DataFrames. So, in addition to starting with a DataFrame, calling registerTempTable, and then executing SQL, like we did above ... we can also go the other direction, starting with SQL and switching to DataFrames whenever we'd like:

// COMMAND ----------

// MAGIC %sql CREATE TEMPORARY TABLE more_people 
// MAGIC USING parquet 
// MAGIC OPTIONS (path "dbfs:/mnt/training/ssn/names.parquet")

// COMMAND ----------

sqlContext.table("more_people").filter($"year" > 2010).show(5)

// COMMAND ----------

// MAGIC %md Because these operations return DataFrames, all the usual DataFrame operations are available.
// MAGIC 
// MAGIC . . . including the ability to create new temporary tables:

// COMMAND ----------

sqlContext.sql("SELECT * FROM more_people WHERE year = 2014").registerTempTable("names2014")

sqlContext.table("names2014").show(5)

// COMMAND ----------

// MAGIC %md ##Window Functions
// MAGIC 
// MAGIC Allow us to query over ranges or ?windows? within data set
// MAGIC * Supported in many SQL database environments
// MAGIC * Any aggregate function can be used, in addition to window-specific ranking and analytic functions
// MAGIC * Spark Window Functions work with SQL or DSL
// MAGIC * In DSL, create a WindowSpec using Window helper object, then use windowfunction?s column .over(windowspec)
// MAGIC 
// MAGIC Here's a typical task: calculate rankings within multiple groups -- in this example, simultaneously query rankings of sports teams in their respective divisions:

// COMMAND ----------

val data = Array(("Bears", "E", 10), ("Lizards", "W", 8), ("Giraffes", "W", 7), ("Tigers", "E", 9), ("Crickets", "E", 6), ("Bats", "W", 6)) 
val df = sc.parallelize(data).toDF("team", "division", "wins")
df.registerTempTable("scores") //for SQL

val rankings = sqlContext.sql("SELECT team, division, wins, rank() OVER (PARTITION BY division ORDER BY wins desc) as rank FROM scores")
rankings.show

// COMMAND ----------

// MAGIC %md In the DataFrame API, we need to create a "WindowSpec" object first, then call ```rank().over(windowSpec)```:

// COMMAND ----------

import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

val ws = Window.partitionBy(df("division")).orderBy(-df("wins"))
df.withColumn("rank", rank().over(ws)).show

// COMMAND ----------

// MAGIC %md ##Supported Window Functions
// MAGIC 
// MAGIC | |SQL|DataFrame API|
// MAGIC |-|---|-------------|
// MAGIC |Ranking functions|rank|rank|
// MAGIC ||dense\_rank|denseRank|
// MAGIC ||percent\_rank|percentRank|
// MAGIC ||ntile|ntile|
// MAGIC ||row\_number|rowNumber|
// MAGIC |Analytic functions|cume\_dist|cumeDist|
// MAGIC ||first\_value|firstValue|
// MAGIC ||last\_value|lastValue|
// MAGIC ||lag|lag|
// MAGIC ||lead|lead|
// MAGIC 
// MAGIC https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html

// COMMAND ----------

// MAGIC %md ##User-Defined Aggregation Functions (UDAF)
// MAGIC 
// MAGIC User-defind functions as discussed earlier apply to one or more columns, but only in one row (record) at a time. So they are, effectively, map functions.
// MAGIC 
// MAGIC ####What about reduce-style custom functions, that can operate on more than one records at a time?
// MAGIC 
// MAGIC If we want to reduce, or aggregate, data from multiple rows in a DataFrame, and we cannot solve the problem with the built-in aggregations, Spark allows us to create User-Defined Aggregation Functions, or UDAFs
// MAGIC 
// MAGIC Since this sort of functions needs to encapsulate a parallelizable reduce, it's a bit more complex. We need to implement a class that extends ```org.apache.spark.sql.UserDefinedAggregateFunction``` and implements 8 abstract methods.
// MAGIC 
// MAGIC Once we have a class that meets the requirements and implements our logic, we can instantiate it and use the instance just as we would one of the built-in aggregations like "sum" or "count" -- e.g., we can groupBy one or more columns, then aggregate over another.

// COMMAND ----------

// MAGIC %md In the example below, we will create a UDAF that collects all of the values of a particular column in all of the rows in a group. That is, we can groupBy one column and then "collect" all of the different values that appear (within the group) in a second column, and place those in an Array. This example will treat those values as Strings, its value will be Array[String]
// MAGIC 
// MAGIC Described as code, we'd like to start with data like
// MAGIC 
// MAGIC ```val a = sc.parallelize(List(("foo", "abc"), ("bar", "abc"), ("foo", "def"))).toDF("name", "bonus")```
// MAGIC 
// MAGIC and then, if we group by the name column, we'll get a row for name "foo" where the aggregated column has a string array of "abc" and "def"
// MAGIC 
// MAGIC Similarly, we could group by the "bonus" column, and then we'll get a row for the bonus value "abc" where the aggregated column has a string array of the name values "foo" and "bar":

// COMMAND ----------

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
 
class Collect extends UserDefinedAggregateFunction {
  def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("name", StringType) :: Nil)
 
  def bufferSchema: StructType = StructType(
    StructField("strings", ArrayType(StringType, false)) :: Nil
  )
 
  def dataType: DataType = ArrayType(StringType)
  def deterministic: Boolean = true
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[String]()
  }
 
  def update(buffer: MutableAggregationBuffer,input: Row): Unit = {
    buffer(0) = buffer.getSeq(0) :+ input.getAs[String](0)
  }
 
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
   buffer1(0) = buffer1.getSeq(0) ++ buffer2.getSeq(0)
  }
 
  def evaluate(buffer: Row): Any = {
    buffer.getAs[Array[String]](0)
  }
}

val collect = new Collect

// COMMAND ----------

// MAGIC %md Now we can use the instance "collect" as a UDAF and aggregate with it:

// COMMAND ----------

val a = sc.parallelize(List(("foo", "abc"), ("bar", "abc"), ("foo", "def"))).toDF("name", "bonus")
a.groupBy($"bonus").agg(collect($"name")).show
a.groupBy($"name").agg(collect($"bonus")).show

// COMMAND ----------

a.groupBy($"name").agg(collect($"bonus")).printSchema

// COMMAND ----------

// MAGIC %md ##Complex Column Types
// MAGIC 
// MAGIC Dataframes can support complex types in a column. For example, a column's type can be a hashmap. It is common to encounter map-typped columns in an existing Hive environment, although Spark's support *does not require Hive*
// MAGIC 
// MAGIC There are several ways to access data in a map-typped column, depending upon whether we want to retrieve the map itself, or instead transform the Dataframe to bring map keys, values, or both up to top-level fields in the schema.

// COMMAND ----------

val df = sc.parallelize(List( ("John", Map("games"->12, "highscore" ->199) ), ("Anne", Map("games"->9, "highscore" ->200 )  ))).toDF("name", "player_info")
df.printSchema
df.show

// COMMAND ----------

// MAGIC %md The following code shows how the map is accessessed within a Row. Although "collect" will rarely be appropriate in big data settings, the focus here is on Row -- a Dataframe is a Dataset[Row] and can be transparently converted to a RDD[Row] if necessary.

// COMMAND ----------

val rows = df.filter($"name" === "John").select($"player_info").collect
val map = rows(0).getMap[String,Int](0)
map("games")

// COMMAND ----------

// MAGIC %md If we know the specific key we need from a map, we can project it into a top-level column using a "." syntax within a column name:

// COMMAND ----------

df.select($"name", $"player_info.highscore").show

// COMMAND ----------

// MAGIC %md We can use the `explode` method to create a custom translation of a Map (or other structure) into one or more columns and/or rows. This is similar to a HiveQL `LATERAL VIEW`
// MAGIC 
// MAGIC First, let's look at making one fixed column with custom String contents for the map: we can make 1 or more Rows from the map.

// COMMAND ----------

df.select($"name", $"player_info").explode("player_info", "entry") {
  player_info_map : Map[String,Int] => {
    player_info_map.keys.map(k => k + " : " + player_info_map(k))
  }
}.show

// COMMAND ----------

// MAGIC %md Sometimes, we'd like to make a number of new columns, perhaps representing specific keys that were in the map. We can do that too:

// COMMAND ----------

case class PlayerInfo(games:Int, highscore:Int)

df.select($"name", $"player_info").explode($"player_info"){
  r: Row => {
    val map = r.getMap[String,Int](0)    
    Seq(PlayerInfo(map("games"), map("highscore")))
  }
}.show

// COMMAND ----------

// MAGIC %md What if we want to expand each key-value pair in into its own row, a bit like a triple-store (https://en.wikipedia.org/wiki/Triplestore) ?
// MAGIC 
// MAGIC We can do that with the same `explode` call, by returning an iterator with different semantics:

// COMMAND ----------

df.select($"name", $"player_info").explode($"player_info"){
  r: Row => {
    val map = r.getMap[String,Int](0)
    map.keys.map(k => (k, map(k)))
  }
}.withColumnRenamed("_1", "map_key").withColumnRenamed("_2", "map_value").show

// COMMAND ----------

// MAGIC %md ##DataFrame Limitations
// MAGIC 
// MAGIC * Spark does not automatically repartition DataFrames optimally during shuffles
// MAGIC   * During a DF shuffle, SparkSQL will just use ```spark.sql.shuffle.partitions``` to determined the number of partitions in the downstream RDD
// MAGIC   * All SQL configurations can be changed, including this one
// MAGIC     * via ```sqlContext.setConf(key, value)```
// MAGIC     * or in Databricks: ```%sql SET key=val```

// COMMAND ----------

// MAGIC %md ##Machine Learning Integration
// MAGIC 
// MAGIC * Spark 1.2 introduced a new package called __spark.ml__, which aims to provide a uniform set of high-level APIs that help users create and tune practical machine learning pipelines.
// MAGIC 
// MAGIC * Spark ML standardizes APIs for machine learning algorithms to make it easier to combine multiple algorithms into a single *pipeline*, or *workflow*.
// MAGIC 
// MAGIC * Spark ML uses DataFrames as a dataset which can hold a variety of data types. 
// MAGIC   * For instance, a dataset could have different columns storing text, feature vectors, true labels, and predictions.
