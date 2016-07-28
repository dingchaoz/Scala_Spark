// Databricks notebook source exported at Tue, 28 Jun 2016 02:23:06 UTC
// MAGIC %md # GraphFrames Demonstration

// COMMAND ----------

// MAGIC %md ## What is GraphFrames?
// MAGIC 
// MAGIC [GraphFrames](http://graphframes.github.io/user-guide.html) is a DataFrame-based API that allows you to analyze graph data (social graphs, network graphs, property graphs, etc.), using the power of the Spark
// MAGIC cluster. It is the successor to the RDD-based [GraphX](http://spark.apache.org/docs/latest/graphx-programming-guide.html) API. For an introduction to GraphFrames, see [this blog post](https://databricks.com/blog/2016/03/03/introducing-graphframes.html).
// MAGIC 
// MAGIC This notebook contains a simple demonstration of just one small part of the GraphFrames API. Hopefully, it'll whet your appetite.

// COMMAND ----------

// MAGIC %md An RDD variant of this demonstration, with a detailed explanation, can be found here: <http://ampcamp.berkeley.edu/big-data-mini-course/graph-analytics-with-graphx.html>. This version is based on the new GraphFrames API.

// COMMAND ----------

// MAGIC %md The following example creates a _property graph_. A property graph is a _directed_ graph: Each node, or _vertex_, in the graph "points" at another vertex, and the edge represents the relationship between the first node and the second node.
// MAGIC 
// MAGIC The image, at the right, shows an example of a property graph. In that example, a portion of a fictitious dating site, there are four people (the vertices), connected by edges that have the following meanings:
// MAGIC 
// MAGIC * Valentino likes Angelica; he rated her 9 stars.
// MAGIC * Angelica also gave Valentino 9 stars.
// MAGIC * Mercedes gave Valentino 8 stars.
// MAGIC * Anthony doesn't really like Angelica at all; he gave her 0 zeros.
// MAGIC 
// MAGIC <img src="http://i.imgur.com/tUNGnru.png" alt="Property Graph Example" style="float: right"/>
// MAGIC 
// MAGIC Let's expand on this World's Most Unpopular Dating Site just a little, for our example.

// COMMAND ----------

// MAGIC %md ## Constructing our graph

// COMMAND ----------

import org.graphframes._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

case class Person(id: Int, name: String, age: Int)

type VertexType = Tuple2[Long, Person]

// These are the people. They will become the vertices. Each person record
// has three fields.
//
// - a unique numeric ID
// - a name
// - an age
val people = Array(
  (1, "Angelica", 23),
  (2, "Valentino", 31),
  (3, "Mercedes", 52),
  (4, "Anthony", 39),
  (5, "Roberto", 45),
  (6, "Julia", 34)
)
// This map makes the edge code easier to read.
val nameToId = people.map { case (id, name, age) => (name, id) }.toMap

// The vertices are the people.
val vertices = sqlContext.createDataFrame(people).toDF("id", "name", "age")

// The edges connect the people, and each edge contains the rating that person 1 gave person 2 (0 to 10).
// The edges use the IDs to identify the vertices.
val edges = sqlContext.createDataFrame(
  Array(
  /* First Person by Index >> related somehow to Second Person by Index, with Attribute Value (which is contextual) */
    (nameToId("Valentino"), nameToId("Angelica"),  9 /* stars */),
    (nameToId("Valentino"), nameToId("Julia"),     2),
    (nameToId("Mercedes"),  nameToId("Valentino"), 8),
    (nameToId("Mercedes"),  nameToId("Roberto"),   3),
    (nameToId("Anthony"),   nameToId("Angelica"),  0),
    (nameToId("Roberto"),   nameToId("Mercedes"),  5),
    (nameToId("Roberto"),   nameToId("Angelica"),  7),
    (nameToId("Angelica"),  nameToId("Valentino"), 9),
    (nameToId("Anthony"),   nameToId("Mercedes"),  1),
    (nameToId("Julia"),     nameToId("Anthony"),   8),
    (nameToId("Anthony"),   nameToId("Julia"),    10)
  )
)
.toDF("src", "dst", "stars")

// The graph is the combination of the vertices and the edges.
val g = GraphFrame(vertices, edges)

// COMMAND ----------

display(g.vertices)

// COMMAND ----------

display(g.edges)

// COMMAND ----------

// MAGIC %md Let's look at the incoming degree of each vertex, which is the number of edges coming _into_ each node (i.e., the number of people who have rated a particular person)

// COMMAND ----------

display(g.inDegrees) // The incoming degree of the vertices

// COMMAND ----------

// MAGIC %md We can make that more readable.

// COMMAND ----------

val inDegrees = g.inDegrees
display(
  inDegrees.join(vertices, inDegrees("id") === vertices("id"))
           .select($"name", $"inDegree".as("number of ratings"))
)

// COMMAND ----------

// MAGIC %md Let's do the same with out degrees, i.e., the number of ratings each person has made.

// COMMAND ----------

val outDegrees = g.outDegrees
display(
  outDegrees.join(vertices, outDegrees("id") === vertices("id"))
            .select($"name", $"outDegree".as("number of ratings"))
)

// COMMAND ----------

// MAGIC %md `degrees` is just the sum of the input and output degrees for each vertex. Or, to put it another way, it's the number of times each person's ID appears as a vertex.

// COMMAND ----------

val degrees = g.degrees
display(
  degrees.join(vertices, degrees("id") === vertices("id"))
         .select($"name", $"degree".as("number of ratings"))
)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ## Who likes whom?
// MAGIC 
// MAGIC We can visually scan the output to find people who really like each other, but only because the graph is so small. In a larger graph, that would be tedious. So, let's see if we can find a way to process the graph programmatically to find people really like each other. We'll define "really like" as two people who have rated each with 8 or more stars.
// MAGIC 
// MAGIC We want to find sets of two people, each of whom rated the other at least 8. [GraphFrame motif finding](http://graphframes.github.io/user-guide.html#motif-finding) can help here. GraphFrame
// MAGIC motif finding uses a simple string-based Domain-Specific Language (DSL) to express structural queries. For instance, the expression `"(a)-[e]->(b); (b)-[e2]->(a)"` will search for pairs of vertices,
// MAGIC `a` and `b`, that are connected by edges in both directions. As it happens, this is _exactly_ what we want.

// COMMAND ----------

val matchUps = g.find("(a)-[ab]->(b); (b)-[ba]->(a)").filter(($"ab.stars" > 7) && ($"ba.stars" > 7))
display(matchUps)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC In that output, we can see, for instance, that Valentino likes Angelica (one row) and Angelica like Valentino (another row). Can we collapse that output down to simple rows of matched pairs?
// MAGIC 
// MAGIC We can, using a little DataFrame manipulation.
// MAGIC 
// MAGIC * Use the `array()` function from `org.apache.spark.sql.functions` to pull the two name columns together into a single array column.
// MAGIC * Use the `sort_array()` function to sort that array column. Thus, the two rows with `[Valentino, Angelica]`, `[Angelica, Valentino]`, will both end up having the value `[Angelica, Valentino]`.
// MAGIC * Use `distinct` to sort remove the duplicate rows. That way, we don't end up with two `[Angelica <-> Valentino]` entries.
// MAGIC 
// MAGIC All three operations can be performed in one statement:
// MAGIC 
// MAGIC ```
// MAGIC val finalMatchups = matchUps.select(sort_array(array($"a.name", $"b.name")).as("names")).distinct
// MAGIC ```
// MAGIC 
// MAGIC However, let's do them one by one, so we can see the transformations.

// COMMAND ----------

val df1 = matchUps.select(array($"a.name", $"b.name").as("names"))
display(df1)

// COMMAND ----------

val df2 = df1.select(sort_array($"names").as("names"))
display(df2)

// COMMAND ----------

val finalMatchups = df2.distinct()
display(finalMatchups)

// COMMAND ----------

// MAGIC %md Finally, let's pull the matches back to the driver and print them. To make the types a little easier to manage, let's map the
// MAGIC `finalMatchUps` DataFrame to a `Dataset[Array[String]]`.

// COMMAND ----------

finalMatchUps.as[Array[String]].collect().foreach { case Array(n1, n2) =>
  println(s"$n1 and $n2 really like each other.")
}

// COMMAND ----------

