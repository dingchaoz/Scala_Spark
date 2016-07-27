// Databricks notebook source exported at Tue, 26 Jul 2016 02:28:30 UTC
// MAGIC %md ## RDD Fundamentals
// MAGIC 
// MAGIC ### Spark - Cluster Architecture
// MAGIC 
// MAGIC Spark divides its compute engine into the following components
// MAGIC * Driver
// MAGIC   * Launch spark applications
// MAGIC   * Can be run outside or inside the cluster
// MAGIC   * Orchestrates or controls what we do with the "big data" but doesn't usually contain the data itself
// MAGIC     * Why? the driver is a single JVM, and if we could fit all of our data in a single JVM we wouldn't need Spark!
// MAGIC     
// MAGIC * Executors
// MAGIC   * JVMs with separate threads (typically 1 per core) to do work in the cluster
// MAGIC   * Executors process units of work called "tasks" which are sent by the driver
// MAGIC   
// MAGIC * Cluster Manager
// MAGIC   * Supplies containers for Executor and/or Driver JVM
// MAGIC     * E.g., in a YARN cluster, executor JVMs run within YARN containers
// MAGIC   * One worker (cluster node) can have one to many executors

// COMMAND ----------

// MAGIC %md ## Spark - Cluster Architecture
// MAGIC 
// MAGIC Exectors and Cores
// MAGIC * Each executor will try to run a configurable number of threads
// MAGIC * We often call these threads "cores" because we typically allocate one thread or "task-slot" per core
// MAGIC * Each core can run any type of task (unlike MapReduce)
// MAGIC 
// MAGIC We start programming Spark by using the SparkContext, which only exists in the driver.
// MAGIC * SparkContext represents a handle or entry-point to the execution environment.
// MAGIC * In standalone code, we import, then instantiate the SparkContext class, 
// MAGIC   * but in the Spark shells and notebooks we already have one: it's called `sc`

// COMMAND ----------

println(sc)
println(sc.version)
println(sc.appName)

// COMMAND ----------

// MAGIC %md Spark comes with interactive shells for Python, Scala, SQL, and R. They look like this:
// MAGIC 
// MAGIC <img src="http://i.imgur.com/I73VwI0.png" width="500">

// COMMAND ----------

// MAGIC %md Running code in a notebook is similar to -- but not exactly the same as -- running it in the Spark shell.
// MAGIC 
// MAGIC No matter how you run Spark, your main program -- which loads, manipulates, and analyzes data -- is run by the Spark driver. Every Spark application has a Driver JVM.
// MAGIC 
// MAGIC Distributed clusters also have 1 or more Executor JVMs to perform parallel work. With some (but not all) Spark APIs, you may provide functions that get sent to the Executors to operate over a chunk of data.
// MAGIC 
// MAGIC The following diagram illustrates:
// MAGIC 
// MAGIC * Spark Driver
// MAGIC * 2 Executors (the yellow boxes)
// MAGIC * Each Executor is running in a Worker Node
// MAGIC  * Note that Spark __does not__ require one node per Executor
// MAGIC  * If your Worker Nodes are large servers, they might run multiple Executors
// MAGIC * The Spark Driver is __not__ the same thing as a cluster "master"
// MAGIC  * Most likely, your Spark Driver will either be outside the cluster altogether, or in some Worker Node
// MAGIC 
// MAGIC <img src="http://i.imgur.com/LsUheja.png" width="700">
// MAGIC 
// MAGIC Let's see how many executors we have, and how much memory they have:

// COMMAND ----------

sc.getExecutorMemoryStatus.foreach(println)

// COMMAND ----------

// MAGIC %md That almost worked. If there's only 1 item appearing, that's actually the driver, and you are running in "Local Mode" -- this means you're using one JVM and simulating Executors using extra threads. It's an easy way to run Spark on your laptop, or for Databricks to provide lightweight Spark access in the cloud.
// MAGIC 
// MAGIC If you see a list, but one of the items appears different, that's also the driver. For a prettier view, you can open the Spark Web UI, and click the Executors tab. Note: you'll still see the driver listed in that table, even though it says "Executors"

// COMMAND ----------

// MAGIC %md Let's do something in parallel with Spark

// COMMAND ----------

val myFirstRDD = sc.parallelize(1 to 25, 5)
myFirstRDD.count

// COMMAND ----------

// MAGIC %md Congratulations! You've just run your first Spark job. Pretty impressive, eh?
// MAGIC 
// MAGIC So what actually happened there? We told Spark to split up our Range (1 to 25) into 5 parts for possible parallel execution. Then we asked Spark to count the items in those 5 parts.
// MAGIC 
// MAGIC (Why "possible" parallel execution? Because if you don't have 5 cores available, one or more of those parts may have had to wait its turn for computation ... We'll discuss that more in Job Execution.)

// COMMAND ----------

// MAGIC %md ##RDD: Resilient Distributed Dataset
// MAGIC 
// MAGIC Spark's fundamental model for distributed data is an abstraction called an RDD. The RDD itself is a small Java/Scala object on the driver, but it __represents__ a potentially huge dataset, broken up into chunks. Each chunk is called a __partition__. When we perform computation on the actual data, that data will be processed in the Executors. The RDD and partition info is the Driver's road map to the data set.

// COMMAND ----------

// MAGIC %md In this diagram, we envision an RDD divided into 5 partitions, and imagine our 25 items in those partitions. We often talk about RDDs and partitions this way, as if they actually contained data ... but they don't really.
// MAGIC 
// MAGIC Each partition is shown with a dotted arrow to a pale blue box in an Executor. That Executor is where we will really process the data.
// MAGIC 
// MAGIC <img src="http://i.imgur.com/sNS5QyK.png" width="700">

// COMMAND ----------

// MAGIC %md ##How do we create RDDs?
// MAGIC 
// MAGIC RDDs can be created 
// MAGIC * by parallelizing a (small) collection from the driver, like we did above
// MAGIC * reading data from a file
// MAGIC * performing an operation (like filter or join) on one or more existing RDDs
// MAGIC 
// MAGIC Let's create an RDD from a text file. If we do that,
// MAGIC * the partitioning depends on the source we use (e.g., if we read from HDFS, we'll get 1 parition per HDFS block)
// MAGIC * each item in the dataset if one line from the file

// COMMAND ----------

val mySecondRDD = sc.textFile("dbfs:/mnt/training/purchases.txt")
println("The first item in this dataset is: " + mySecondRDD.first)
println("The number of items is: " + mySecondRDD.count)
println("There are " + mySecondRDD.getNumPartitions + " partitions")

// COMMAND ----------

// MAGIC %md Let's filter this dataset, just to see how we can make a new dataset by filtering:

// COMMAND ----------

def purchaseIs10Dollars(s:String) = s.startsWith("10,")

val myThirdRDD = mySecondRDD.filter(purchaseIs10Dollars)
println("The first item in this dataset is: " + myThirdRDD.first)
println("The number of items is: " + myThirdRDD.count)

// COMMAND ----------

// MAGIC %md ##RDD
// MAGIC 
// MAGIC RDDs represent partitioned collections of objects spread across a cluster, stored in memory or on disk.
// MAGIC Spark programs can be written in terms of operations on RDDs, like our simple count example above.
// MAGIC RDDs are *immutable* once created -- if we operate on them, we get new RDDs. Why? Immutability removes many of the challenges of parallel computation, making it easier for Spark to automatically reconstruct partitions of a dataset if JVMs or Nodes fail.
// MAGIC 
// MAGIC #### What are the key properties of RDDs?
// MAGIC RDDs have four key properties:
// MAGIC * Partitioning: what chunks make up this dataset?
// MAGIC * Parent(s): if we created this dataset by operating on one or more RDDs, what are those?
// MAGIC * Compute: how exactly do we derive this RDD from the parent(s)?
// MAGIC * Locality Preference: where would we, ideally, schedule work on each chunk of data? E.g., if a chunk is an HDFS block, it makes sense to try to schedule work on that chunk on a Node which contains one of the HDFS replicas of the block.

// COMMAND ----------

// MAGIC %md ### How can we use them to answer questions about data?
// MAGIC 
// MAGIC RDDs support two main kinds of operations:
// MAGIC * Transformations, which create new RDDs
// MAGIC   * transformation calls returns new RDD metadata but are *lazy* about operating on the real ("big") data
// MAGIC   * examples include filtering an RDD, joining two RDDs, sorting an RDD, etc.
// MAGIC   
// MAGIC * Actions, which produce a non-RDD result (such as a native, non-Spark List; a numeric value, or "Unit")
// MAGIC   * actions are not lazy, they're computed immediately -- and that means they force computation of any transformations that led up to them
// MAGIC   * examples include count, sum, and saveAsTextFile
// MAGIC 
// MAGIC Let's run another simple example:

// COMMAND ----------

val myLogRDD = sc.parallelize(Seq("INFO", "ERROR", "ERROR", "WARN", "INFO", "WARN", "ERROR", "INFO"))

val myErrorsRDD = myLogRDD.filter(_ == "ERROR") // this is a transformation: we get an RDD right away, but no data actually gets "filtered"

val errorCount = myErrorsRDD.count // count is an action -- it runs right away, and to produce its numeric result, it forces the above filtering to take place

// We can also take any RDD and run additional operations on, for example count all of the different kinds of items

val itemCounts = myLogRDD.countByValue // countByValue is an action: it returns a native (Scala) Map
itemCounts.foreach(println)

// COMMAND ----------

// MAGIC %md ##Here is a diagram showing a similar RDD example
// MAGIC 
// MAGIC 1. We create a log RDD from an HDFS file: 4 HDFS blocks means 4 partitions
// MAGIC 2. We filter the RDD to keep errors. Note that
// MAGIC   * One partition ends up empty. This is not ideal for performance, but it *is* allowed.
// MAGIC   * The partitions do not have the same number of items anymore. This imbalance is called *skew* and when it is severe, it can impact performance.
// MAGIC 
// MAGIC <img src="http://i.imgur.com/pwSTt2x.png" width="700">

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC __ ... continuing ... __
// MAGIC 
// MAGIC 1. Now imagine that we want to reduce the number of partitions, so that we have fewer empty ones. We can use the `coalesce` transformation
// MAGIC   * Why two partitions? In real applications, we would need to understand the distribution of our data and the behavior of our program to derive this number.
// MAGIC   * There's no magic way to determine it, and Spark will not figure it out for you.
// MAGIC 2. Then we call the `collect` action to bring all of the errors into the driver
// MAGIC   * Collect creates a native collection (e.g., Scala Array)
// MAGIC   * Collect is convenient for demos and testing, but in real applications it only makes sense if your dataset has been reduced to very small size.
// MAGIC   * Although the original data is processed across the Executors, once you call `collect` it all has to fit in the driver JVM memory!
// MAGIC 
// MAGIC <img src="http://i.imgur.com/ZuViM3L.png" width="700">

// COMMAND ----------

// MAGIC %md ##Actions force Evaluation of the DAG
// MAGIC 
// MAGIC * Although we thought about our algorithm step by step from reading the file through to collecting, Spark did not actually process the data until we called the action (`collect`)
// MAGIC * So running transformations on RDDs really just adds new RDDs to a data structure for *future* execution
// MAGIC   * This data structure is called the DAG, or Directed Acyclic Graph, because of its shape
// MAGIC * This "lazy execution pattern," which also shows up in other functional platforms like Java 8 Streams, allows some optimizations
// MAGIC 
// MAGIC <img src="http://i.imgur.com/HnDeS2k.png" width="700">

// COMMAND ----------

// MAGIC %md We can see lazy execution in action -- and see one of the gotchas -- in the following example.
// MAGIC 
// MAGIC First, run the next cell:

// COMMAND ----------

val someRDD = sc.textFile("/tmp/records.csv")
val filteredRdd = someRDD.filter(_.startsWith("January"))

// COMMAND ----------

// MAGIC %md So far so good ... we have a couple of RDDs. Let's count the filtered records:

// COMMAND ----------

filteredRdd.count

// COMMAND ----------

// MAGIC %md Well that didn't work. The error message is fairly clear: our input file path doesn't exist.
// MAGIC 
// MAGIC *But notice that we didn't see this error until we tried to run the `count` action!*
// MAGIC 
// MAGIC Lazy evaluation means that the code which throws the error may not be the code which is actually responsible for the error.
// MAGIC 
// MAGIC This example also show the laziness at work: if Spark eagerly tried to materialize `someRDD`, we would have seen the error in the earlier cell.

// COMMAND ----------

// MAGIC %md ##DAG versus Lineage
// MAGIC 
// MAGIC <img src="http://i.imgur.com/b6Yb3Tn.png" width="700">
// MAGIC 
// MAGIC Summing up...
// MAGIC * DAG and Lineage both refer to a number of RDDs linked in a data structure
// MAGIC * the DAG is "the entire family tree" of RDDs
// MAGIC * each RDD has a Lineage which is just its own ancestors (or sometimes just parents, since the parents link to their own parents, etc.)
// MAGIC   * the RDD's Lineage contains all the info Spark needs to materialize or reconstruct any partition in the RDD
// MAGIC   * this allows reconstruction on failure, as well as the ability to speculatively launch an "extra" copy of a task to operate on a partition if desired

// COMMAND ----------

// MAGIC %md ## Although we think about our program and data-flow step-by-step going forward...
// MAGIC 
// MAGIC <img src="http://i.imgur.com/79Q8x6x.png" width="700">

// COMMAND ----------

// MAGIC %md ##... when Spark needs to materialize the data,
// MAGIC ##it works backward from the RDD on which we've called an Action
// MAGIC 
// MAGIC Spark needs 1 or more partitions of that (last) RDD ... where does it get them? from the parent ... and so on back to the data source.
// MAGIC 
// MAGIC <img src="http://i.imgur.com/M8kiJ9v.png" width="700">

// COMMAND ----------

// MAGIC %md Run the following cell -- which is the same code that failed earlier. 
// MAGIC 
// MAGIC This time, expand the stack trace and try to make sense of it from the bottom up: you can see how Spark runs a job for `count`, which leads backwards through the DAG until it fails on the `HadoopRDD`. 
// MAGIC 
// MAGIC Under the hood, HadoopRDD handles file input because Spark uses Hadoop InputFormat code for compatibility with various filesystems.

// COMMAND ----------

filteredRdd.count

// COMMAND ----------

// MAGIC %md ##When the Action is completed, those intermediate RDDs are no longer needed
// MAGIC 
// MAGIC <img src="http://i.imgur.com/sJM2xfN.png" width="700">

// COMMAND ----------

// MAGIC %md ##But what if we want to re-use one of those intermediate RDDs?
// MAGIC 
// MAGIC Maybe we'd like to perform multiple actions on it, like
// MAGIC * save to Amazon S3
// MAGIC * *and* count the items
// MAGIC * *and* filter for specific contents and then collect
// MAGIC 
// MAGIC <img src="http://i.imgur.com/Er69WWr.png" width="700">

// COMMAND ----------

// MAGIC %md ##We improve performance in this case by asking Spark to cache() an RDD
// MAGIC 
// MAGIC * Caching is lazy
// MAGIC   * data doesn't actually get materialized and stored until an Action is run
// MAGIC   * then it will be available for subsequent operations
// MAGIC * There are various ways to store data -- we'll revisit the details later
// MAGIC * If we don't have enough memory for everything we're trying to cache, Spark evicts partitions based on a Least-Recently-Used approach
// MAGIC * All caching or eviction is done at the granularity of a whole partition
// MAGIC   * Why? Because that's the granularity we can safely reconstruct -- it's described by our immutable data structure
// MAGIC   * (Spark was created to avoid the "arbitrary records in memory with unknown state" problem of some distributed memory systems)
// MAGIC 
// MAGIC <img src="http://i.imgur.com/XJRmqJI.png" width="700">

// COMMAND ----------

// MAGIC %md Let's load some sample purchase data and run 5 jobs to calculate values on it. Note the time it takes without caching:

// COMMAND ----------

val allPurchasesRDD = sc.textFile("dbfs:/mnt/training/purchases.txt").map(_.split(",")(0).toInt)
val numPurchases = allPurchasesRDD.count
val maxPurchase = allPurchasesRDD.max
val minPurchase = allPurchasesRDD.min
val meanPurchase = allPurchasesRDD.mean
val totalPurchases = allPurchasesRDD.sum

// COMMAND ----------

// MAGIC %md Now we'll cache the RDD and run the same jobs. Since `cache` is lazy, the first job (the `count`) won't have the benefit of cached data, but the subsequent jobs will. Re-using the in-memory, cached RDD is consistently quicker even for these simple jobs and tiny dataset.

// COMMAND ----------

val allPurchasesRDD = sc.textFile("dbfs:/mnt/training/purchases.txt").map(_.split(",")(0).toInt).cache
val numPurchases = allPurchasesRDD.count
val maxPurchase = allPurchasesRDD.max
val minPurchase = allPurchasesRDD.min
val meanPurchase = allPurchasesRDD.mean
val totalPurchases = allPurchasesRDD.sum

// COMMAND ----------

// MAGIC %md ##... and that's the Basic Lifecycle of a Spark Program:
// MAGIC 
// MAGIC 1. Construct "Base" RDD by reading data from stable storage
// MAGIC 2. Apply any number of transformations
// MAGIC 3. (Optionally) cache data that will be re-used
// MAGIC 4. Call actions to get results on the driver or persist to stable storage
// MAGIC 
// MAGIC We can extend this flow 
// MAGIC * by applying more transformations and actions
// MAGIC * using arbitrary control constructs in code! Loops, branches, classes, functions?

// COMMAND ----------

// MAGIC %md #### One last time: we think about processing data going forward through a flow...
// MAGIC 
// MAGIC <img src="http://i.imgur.com/M65hs2o.png" width="700">

// COMMAND ----------

// MAGIC %md #### ... which builds a DAG that Spark uses to work backwards to calculate results
// MAGIC 
// MAGIC <img src="http://i.imgur.com/lOoMq5I.png" width="700">
