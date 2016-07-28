// Databricks notebook source exported at Wed, 27 Jul 2016 13:12:20 UTC
// MAGIC %md ##Introduction
// MAGIC 
// MAGIC Here we will utilize data about old automobiles to predict each automobiles mpg.

// COMMAND ----------

// MAGIC %md Upload data to Databricks and create data table.

// COMMAND ----------

// MAGIC %md Use the automatically created SQLContext then load and Read initial data set from table location. Alternatively, you can directly load the csv file contents.

// COMMAND ----------

val autoData = sqlContext.read
                         .format("csv")
                         .load("/FileStore/tables/fm3i8kkk1468712270369/")
                         //.toDF("mpg", "cylinders", "displacement", "hp", "weight", "acceleration", "year", "origin", "car")
autoData.show(10, false)

// COMMAND ----------

autoData.take(1).foreach(println)

// COMMAND ----------

// MAGIC %md Create a method to process the incoming data and output well defined data with the associated schema.

// COMMAND ----------

case class Auto(mpg: Double, cylinders: Double, displacement: Double, hp: Double, weight: Double, acceleration: Double, year: Double, origin: Double, car: String)
def processData(line: String) = {
  val parts = line.replace("[", "")
                  .replace("]", "")
                  .split(",")
  
  Auto(parts(0).toDouble, parts(1).toDouble, parts(2).toDouble, parts(3).toDouble, parts(4).toDouble, parts(5).toDouble, parts(6).toDouble, parts(7).toDouble, parts(8))
}

// COMMAND ----------

// MAGIC %md Map the data into the new case class. What could go wrong here?
// MAGIC 
// MAGIC In the code below, why can we create a DataFrame without providing a "schema"?

// COMMAND ----------

val newAutoData = autoData.map(row => processData(row.toString)).toDF()
newAutoData.take(20).foreach(println)

// COMMAND ----------

// MAGIC %md Now let us check the "schema" that we did not explicitly provide.

// COMMAND ----------

newAutoData.printSchema

// COMMAND ----------

// MAGIC %md That schema looks familiar! Where have we seen it before?
// MAGIC 
// MAGIC Now let us force Spark to evaluate the data set. YES, that will help us find out the answer to our previous question... what could go wrong?
// MAGIC 
// MAGIC To find out, let us run an "action"...

// COMMAND ----------

newAutoData.count

// COMMAND ----------

// MAGIC %md How can we change the code below to fix this problem?

// COMMAND ----------

def processData(line: String) = {
  val parts = line.replace("[", "")
                  .replace("]", "")
                  //TO DO: Add Code Here!
                  
  
  Auto(parts(0).toDouble, parts(1).toDouble, parts(2).toDouble, parts(3).toDouble, parts(4).toDouble, parts(5).toDouble, parts(6).toDouble, parts(7).toDouble, parts(8))
}

// COMMAND ----------

// MAGIC %md Now, let us see if we have resolved the issue...

// COMMAND ----------

val newAutoData = autoData.map(row => processData(row.toString)).toDF()
newAutoData.count
newAutoData.take(20).foreach(println)

// COMMAND ----------

val autoDS = newAutoData.as[Auto]

// COMMAND ----------

autoDS.show(10, false)


// COMMAND ----------

autoDS.first.mpg

// COMMAND ----------

import org.apache.spark.mllib.linalg.{Vector, Vectors}

// COMMAND ----------

// No need to utilize Datasets here. I have included them for demonstration purposes.
var autoVectors = autoDS.map(row => (row.mpg , Vectors.dense(row.cylinders, row.displacement, row.hp, row.weight, row.acceleration, row.year))).toDF()
autoVectors = autoVectors.toDF()
                         .withColumnRenamed("_1", "label")
                         .withColumnRenamed("_2", "features")
autoVectors.show(false)

// COMMAND ----------

// MAGIC %md Can you rewrite the code in the above cell in a different way? Do not look at the answer below! (Hint: you can define your own method)

// COMMAND ----------

autoDS.first.displacement

// COMMAND ----------

//TO DO: Create a case class for the Auto Vector
case class AutoVector(label: Double, features: Vector)
def vectorizeData(input: Row) = {
  //TO DO: write the method code
  val parts = input.toString.replace("[", "").replace("]", "").split(",")
  val label = parts(0).toDouble
  val features = Vectors.dense(parts(1).toDouble, 
                               parts(2).toDouble, 
                               parts(3).toDouble, 
                               parts(4).toDouble, 
                               parts(5).toDouble, 
                               parts(6).toDouble)
  (label, features)
}

var autoVectors2 = autoDS.toDF().map(row => vectorizeData(row))
autoVectors2.take(10).foreach(println)

// COMMAND ----------

// MAGIC %md Next let us create a Linear Regression object and check out its possible parameters.

// COMMAND ----------

import org.apache.spark.ml.regression.LinearRegression

// Create a LinearRegression instance.  This instance is an Estimator.
val lr = new LinearRegression()

// Print out the parameters, documentation, and any default values.
println("LinearRegression parameters:\n" + lr.explainParams() + "\n")


// COMMAND ----------

// MAGIC %md Split the data (80/20) into training and test sets.

// COMMAND ----------

val splits = autoVectors.randomSplit(Array(0.8, 0.2))
val (trainingData, testData) = (splits(0), splits(1))

// COMMAND ----------

// MAGIC %md Do not forget to set the appropriate parameters before the training is started.

// COMMAND ----------

// We may set parameters using setter methods.
lr.setMaxIter(10)
  .setRegParam(0.01)

val model1 = lr.fit(trainingData)
// Since model1 is a Model (i.e., a Transformer produced by an Estimator),
// we can view the parameters it used during fit().
// This prints the parameter (name: value) pairs, where names are unique IDs for this
// LinearRegression instance.
println("Model 1 was fit using parameters: " + model1.parent.extractParamMap)


// COMMAND ----------

// MAGIC %md Now let us try to run the test data through our model.

// COMMAND ----------

model1.transform(testData)
  .select("features", "label", "prediction")
  .collect()
  .foreach { case Row(features: Vector, label: Double, prediction: Double) =>
    println(s"($features, $label) -> prediction=$prediction")
  }

// COMMAND ----------

// MAGIC %md STOP Here.

// COMMAND ----------

// MAGIC %md Now, let us put our Linear Regression example all together using Pipelines.

// COMMAND ----------

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Row

// COMMAND ----------

// MAGIC %md Let us create the object and set the parameters here

// COMMAND ----------

val lr = new LinearRegression()
  .setMaxIter(10)
  .setRegParam(0.01)
val pipeline = new Pipeline()
  .setStages(Array(lr))

// Fit the pipeline to training data.
val model = pipeline.fit(trainingData)

// COMMAND ----------

// MAGIC %md Run the test set

// COMMAND ----------

// Make predictions on test documents.
model.transform(testData)
  .select("label", "features", "prediction")
  .collect()
  .foreach { case Row(label: Double, features: Vector, prediction: Double) =>
    println(s"($label, $features) --> prediction=$prediction")
  }

// COMMAND ----------

// MAGIC %md Later we will build more complex models, but the idea will remain the same!
