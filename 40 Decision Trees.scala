// Databricks notebook source exported at Wed, 27 Jul 2016 18:25:54 UTC
// MAGIC %md ##Document Classification using Decision Trees
// MAGIC 
// MAGIC This notebook will provide a brief algorithm summary, links for further reading, and an example of how to use Decision Trees for Document Classification.

// COMMAND ----------

// MAGIC %md ####Algorithm Summary
// MAGIC 
// MAGIC     Task: Identify the classes/categories for a collection of text documents
// MAGIC     Input: Vectors of word counts
// MAGIC     Optimizers:

// COMMAND ----------

// MAGIC %md ####Links
// MAGIC 
// MAGIC     Spark API docs
// MAGIC         RDD-based spark.mllib API
// MAGIC             Scala: Decision Trees
// MAGIC             MLlib Programming Guide
// MAGIC         DataFrame-based spark.ml API (used in this notebook)
// MAGIC             Scala: Decision Trees
// MAGIC             ML Programming Guide
// MAGIC     ML Feature Extractors & Transformers
// MAGIC     Wikipedia: Latent Dirichlet Allocation

// COMMAND ----------

// MAGIC %md ####Text Document Classification Example
// MAGIC 
// MAGIC This is an outline of our Document Classification workflow. Feel free to jump to any subtopic to find out more.
// MAGIC 
// MAGIC     Dataset Review
// MAGIC     Loading the Data and Data Cleaning
// MAGIC     Text Tokenization
// MAGIC     StopWords Removal
// MAGIC     Token Counts Vecorization
// MAGIC     Create Decision Tree Model
// MAGIC     Review Results
// MAGIC     Model Tuning - Refilter Stopwords
// MAGIC     Visualize Results

// COMMAND ----------

// MAGIC %md ####Reviewing our Data
// MAGIC 
// MAGIC In this example, we will use the mini 20 Newsgroups dataset, which is a random subset of the original 20 Newsgroups dataset. Each newsgroup is stored in a subdirectory, with each article stored as a separate file.
// MAGIC 
// MAGIC The mini dataset consists of 100 articles from the following 20 Usenet newsgroups:
// MAGIC 
// MAGIC alt.atheism
// MAGIC comp.graphics
// MAGIC comp.os.ms-windows.misc
// MAGIC comp.sys.ibm.pc.hardware
// MAGIC comp.sys.mac.hardware
// MAGIC comp.windows.x
// MAGIC misc.forsale
// MAGIC rec.autos
// MAGIC rec.motorcycles
// MAGIC rec.sport.baseball
// MAGIC rec.sport.hockey
// MAGIC sci.crypt
// MAGIC sci.electronics
// MAGIC sci.med
// MAGIC sci.space
// MAGIC soc.religion.christian
// MAGIC talk.politics.guns
// MAGIC talk.politics.mideast
// MAGIC talk.politics.misc
// MAGIC talk.religion.misc
// MAGIC 
// MAGIC Some of the newsgroups seem pretty similar on first glance, such as comp.sys.ibm.pc.hardware and comp.sys.mac.hardware, which may affect our results.
// MAGIC Loading the Data and Data Cleaning

// COMMAND ----------

// MAGIC %md We will use the wget command to download the file, and read in the data using wholeTextFiles().

// COMMAND ----------

// MAGIC %sh wget http://kdd.ics.uci.edu/databases/20newsgroups/mini_newsgroups.tar.gz -O /tmp/newsgroups.tar.gz

// COMMAND ----------

// MAGIC %md Untar the file into the /tmp/ folder.

// COMMAND ----------

// MAGIC %sh tar xvfz /tmp/newsgroups.tar.gz -C /tmp/

// COMMAND ----------

// MAGIC %md The below cell takes about 10 mins to run.

// COMMAND ----------

// MAGIC %fs cp -r file:/tmp/mini_newsgroups dbfs:/tmp/mini_newsgroups

// COMMAND ----------

// MAGIC %md The wholeTextFiles() command will read in the entire directory of text files, and return a key-value pair of (filePath, fileContent).
// MAGIC 
// MAGIC As we do not need the file paths in this example, we will apply a map function to extract the file contents, and then convert everything to lowercase.

// COMMAND ----------

// Load text file, leave out file paths, convert all strings to lowercase

val corpus = sc.wholeTextFiles("/tmp/mini_newsgroups/*").map(_._2).map(_.toLowerCase())

// COMMAND ----------

val temp = sc.wholeTextFiles("/tmp/mini_newsgroups/*")

// COMMAND ----------

val temp2 = temp.map(row => (row._1.split("/")(3), row._2))

val corpus = temp2.map(row => (row._1, row._2.split("\\n\\n").drop(1).mkString(" ") ))

// COMMAND ----------

corpus.first._2

// COMMAND ----------

// MAGIC %md Note that the document began with a header containing some metadata that we don't need, and we are only interested in the category and the body of the document. We just did a bit of simple data cleaning here by removing the metadata of each document whhile extracting the category, which reduces the noise in our dataset. This is an important step as the accuracy of our models depend greatly on the quality of data used.

// COMMAND ----------

// Review first 5 documents with metadata removed

corpus.take(5).foreach(println)

// COMMAND ----------

// MAGIC %md To use the convenient Feature extraction and transformation APIs, we have to convert our RDD into a DataFrame.
// MAGIC 
// MAGIC We will also create an ID for every document.

// COMMAND ----------

val corpusDF = corpus.toDF("label", "content")
corpusDF.printSchema
corpusDF.first

// COMMAND ----------

// MAGIC %md ####Text Tokenization
// MAGIC 
// MAGIC We will use the RegexTokenizer to split each document into tokens. We can setMinTokenLength() here to indicate a minimum token length, and filter away all tokens that fall below the minimum.

// COMMAND ----------

import org.apache.spark.ml.feature.RegexTokenizer

// Set params for RegexTokenizer
val tokenizer = new RegexTokenizer()
  .setPattern("[\\W_]+")
  .setMinTokenLength(4) // Filter away tokens with length < 4
  .setInputCol("content")
  .setOutputCol("tokens")

// Tokenize document
val tokenizedDF = tokenizer.transform(corpusDF)

// COMMAND ----------

display(tokenizedDF.select("tokens"))

// COMMAND ----------

// MAGIC %md #####Remove Stopwords
// MAGIC 
// MAGIC We can easily remove stopwords using the StopWordsRemover(). If a list of stopwords is not provided, the StopWordsRemover() will use this list of stopwords by default. You can use getStopWords() to see the list of stopwords that will be used.
// MAGIC 
// MAGIC In this example, we will specify a list of stopwords for the StopWordsRemover() to use. We do this so that we can add on to the list later on.

// COMMAND ----------

// MAGIC %sh wget http://ir.dcs.gla.ac.uk/resources/linguistic_utils/stop_words -O /tmp/stopwords

// COMMAND ----------

// MAGIC %fs cp file:/tmp/stopwords dbfs:/tmp/stopwords

// COMMAND ----------

// List of stopwords
val stopwords = sc.textFile("/tmp/stopwords").collect()

// COMMAND ----------

import org.apache.spark.ml.feature.StopWordsRemover

// Set params for StopWordsRemover

val remover = new StopWordsRemover()
  .setStopWords(stopwords) // This parameter is optional
  .setInputCol("tokens")
  .setOutputCol("filtered")

// Create new DF with Stopwords removed

val filteredDF = remover.transform(tokenizedDF)

// COMMAND ----------

filteredDF.show(5)

// COMMAND ----------

import org.apache.spark.ml.feature.{StringIndexer, IndexToString, VectorIndexer}

// Index labels, adding metadata to the label column.
// Fit on whole dataset to include all labels in index.
val labelIndexer = new StringIndexer()
  .setInputCol("label")
  .setOutputCol("indexedLabel")
  .setHandleInvalid("skip")
  .fit(filteredDF)

// val indexedCorpus = labelIndexer.transform(filteredDF)

// COMMAND ----------

import org.apache.spark.mllib.linalg.{Vector, Vectors}

// COMMAND ----------

filteredDF.printSchema

// COMMAND ----------

// MAGIC %md We no longer need the original tokens

// COMMAND ----------

// case class DocVector(label: String, content: String, tokens: Vector.dense)
val finalDF = filteredDF.drop($"tokens")

// COMMAND ----------

import org.apache.spark.ml.feature.CountVectorizer

// Set params for CountVectorizer

val vectorizer = new CountVectorizer()
  .setInputCol("filtered")
  .setOutputCol("features")
  .setVocabSize(10000)
  .setMinDF(5)
  .fit(finalDF)

// COMMAND ----------

// MAGIC %md Divide the data into a training and a test set

// COMMAND ----------

// Split the data into training and test sets (30% held out for testing)
val Array(trainingData, testData) = finalDF.randomSplit(Array(0.7, 0.3))

// COMMAND ----------

// MAGIC %md Create the Decision Tree

// COMMAND ----------

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassificationModel

// Train a DecisionTree model

val dt = new DecisionTreeClassifier()
  .setLabelCol("indexedLabel")
  .setFeaturesCol("features")

// COMMAND ----------

// MAGIC %md Create the converter to convert the labels back

// COMMAND ----------

// Convert indexed labels back to original labels.
val labelConverter = new IndexToString()
  .setInputCol("prediction")
  .setOutputCol("predictedLabel")
  .setLabels(labelIndexer.labels)

// COMMAND ----------

// MAGIC %md Create the ML Pipeline with four stages

// COMMAND ----------

import org.apache.spark.ml.Pipeline

// Chain indexers and tree in a Pipeline
val pipeline = new Pipeline()
  .setStages(Array(labelIndexer, vectorizer, dt, labelConverter))

// COMMAND ----------

// Train model.  This also runs the indexers.
val model = pipeline.fit(trainingData)

// COMMAND ----------

// MAGIC %md Let us check the stages of the model we just run.

// COMMAND ----------

model.stages

// COMMAND ----------

// MAGIC %md Next, apply the model trained to the test set.

// COMMAND ----------

// Make predictions
val predictions = model.transform(testData)

// COMMAND ----------

predictions.cache()

// COMMAND ----------

// MAGIC %md Take a look at the predictions. Not great. Why? Is it our model? Or is it something else?

// COMMAND ----------

predictions.show(100)

// COMMAND ----------

// MAGIC %md Let us make the resumts more readable

// COMMAND ----------

// Select example rows to display.
predictions.select("predictedLabel", "label", "features").show(200)

// COMMAND ----------

// MAGIC %md Next, an evaluator is created to assess the results

// COMMAND ----------

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
// Select (prediction, true label) and compute test error
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("indexedLabel")
  .setPredictionCol("prediction")
  .setMetricName("precision")
val accuracy = evaluator.evaluate(predictions)
println("Test Error = " + (1.0 - accuracy))

val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
println("Learned classification tree model:\n" + treeModel.toDebugString)

// COMMAND ----------

// MAGIC %md What are you conclusions? Note that this is the same dataset we used for the topic modeling example.
