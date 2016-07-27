// Databricks notebook source exported at Thu, 10 Dec 2015 18:29:08 UTC
import com.databricks.training.test._
val test = Test()

// COMMAND ----------

class WordCounter {
  import scala.collection.mutable.Map

  /** Given a sentence, count the number of occurrences of
    * each word in the sentence, returning a map of (word -> count)
    * instances.
    */
  def countWords(sentence: String): Map[String, Int] = {
    val result = Map.empty[String,Int]
    
    // YOUR CODE HERE
    
    result
  }

  private def splitSentence(s: String): Array[String] = {
    // Regular expression magic.
    """[\s\W]+""".r.split(s)
  }
}

// COMMAND ----------

val counter = new WordCounter
val res1 = counter.countWords("Red sky at night, sailors delight; red sky at morning, sailors take warning").toMap
test.assertEquals(
  res1,
  Map("red" -> 2, "sky" -> 2, "at" -> 2, "night" -> 1, "morning" -> 1, "delight" -> 1,
      "sailors" -> 2, "take" -> 1, "warning" -> 1),
  "Not correct.")

val res2 = counter.countWords("This is a valid sentence: Buffalo buffalo buffalo Buffalo buffalo.")
test.assertEquals(
  res2,
  Map("buffalo" -> 5, "this" -> 1, "is" -> 1, "a" -> 1, "valid" -> 1, "sentence" -> 1),
  "Not correct")

// COMMAND ----------

