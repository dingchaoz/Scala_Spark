// Databricks notebook source exported at Thu, 10 Dec 2015 18:22:52 UTC
class WordCounter {
  import scala.collection.mutable.Map

  /** Given a sentence, count the number of occurrences of
    * each word in the sentence, returning a map of (word -> count)
    * instances.
    */
  def countWords(sentence: String): Map[String, Int] = {
    val result = Map.empty[String,Int]
    val words = splitSentence(sentence)
    for (word <- words) {
      val lcWord = word.toLowerCase
      result += lcWord -> (1 + result.getOrElse(lcWord, 0))
    }
    result
  }

  private def splitSentence(s: String): Array[String] = {
    // Regular expression magic.
    """[\s\W]+""".r.split(s)
  }
}