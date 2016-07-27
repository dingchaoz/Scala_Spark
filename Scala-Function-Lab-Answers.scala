// Databricks notebook source exported at Thu, 10 Dec 2015 18:37:32 UTC
// MAGIC %md # Scala Function Lab

// COMMAND ----------

import com.databricks.training.test._
val test = new Test

// COMMAND ----------

// MAGIC %md
// MAGIC Create a function which takes an `Int` and returns the square of that `Int`. Then, create a `Vector` with 5 integers. (Use the values 1, 3, 5, 7, and 9.) On each item in the `Vector`, call your squaring function and collect the results in a new `Vector`.
// MAGIC 
// MAGIC **Hint**: Look up the `:+` operator in the [Vector](http://scala-lang.org/files/archive/api/current/#scala.collection.immutable.Vector) docs

// COMMAND ----------

def mySquare(n: Int): Int = {
  n * n
}

val v: Vector[Int] = Vector(1, 3, 5, 7, 9)
var v2: Vector[Int] = Vector()

for (i <- v) {
  v2  = v2 :+ mySquare(i)
}

// COMMAND ----------

test.assertEquals(v2, Vector(1, 9, 25, 49, 81), "Not correct")

// COMMAND ----------

// MAGIC %md 
// MAGIC Now call `.map()` on the original Vector and pass your function as a param

// COMMAND ----------

val v3 = v.map(mySquare)

// COMMAND ----------

test.assertEquals(v3, Vector(1, 9, 25, 49, 81), "Not correct")

// COMMAND ----------

// MAGIC %md Try rewriting the `.map()` call so that it uses a lambda, instead of calling your function.

// COMMAND ----------

val v4 = v.map(n => n * n)

// COMMAND ----------

test.assertEquals(v4, Vector(1, 9, 25, 49, 81), "Not correct")

// COMMAND ----------

