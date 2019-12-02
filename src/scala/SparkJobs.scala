package cs1699
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object SparkJobs {
  // SparkContext for the class
  var sc: SparkContext

  def main (args: Array[String]): Unit = {
    if (args.length < 3) {
      throw new IllegalArgumentException("At least 3 arguments are required: <inputPath> <outputPath> <algorithm> <OPTIONAL: addtional parameter>")
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val algorithm = args(2)


    //Right now this is a wordcount program
    val sc = new SparkContext(new SparkConf().setAppName("InvertedIndex"))

    if (algorithm == "invertedIndex"){
      invertedIndex(inputPath, outputPath)
      /** TODO: Return Success */
    }
    else if (algorithm == "topN"){
      val param = args(3)
      val ret = topN(inputPath, outputPath, param)
      ret.saveAsTextFile(outputPath)
      /** TODO: Return Success */
    }
    else if (algorithm == "searchForTerm"){
      val param = args(3)
      val ret = searchForTerm(inputPath, outputPath, param)
      ret.saveAsTextFile(outputPath)
      /** TODO: Return Success */
    }
    else{
      throw new IllegalArgumentException("Invalid: Algorithm must either 'invertedIndex', 'topN', or 'searchForTerm' ")
    }

  }

  def invertedIndex(inputPath: String, outputPath: String): Unit = {
    val file = sc.wholeTextFiles(inputPath) //file = (path, filecontents)
    val words = file._2.split(" ").map(a => (a, file._1)) //words = List((word, path))
    /**
      * Create a Tuple: ((word, path), 1) "lines.map"
      * Sum tuple values: ((word, path), n) ".reduceByKey"
      * Keys go by Words, and emit (word, (path, n)) ".map"
      * Make lists of (path, n) for each word and emit (word, List(path, n)) ".groupByKey"
      * Count the number of times each word appeared and combine with w, emit ((word, total), SortedList(path, n)) ".map"
      * Sort output by the total counts, emit (((word, highcount), SortedList(path, n)), ... ((word, lowcount), SortedList(path, n))), ".sortBy"
    */
    val output = words.map(a => (a, 1)).reduceByKey(_ + _).map{
      case((w, p), n) => (w, (p, n))
    }.groupByKey().map {
      case (w, l) => ((w, l.reduce{
        case (a, b) => (a._2 + b._2)
      }), l.sortBy(_._2))
    }.sortBy{
      case ((w, t), l) => t
    }.saveAsTextFile(outputPath)
  }

  def topN(inputPath: String, outputPath: String, n: Int): List[Tuple[Tuple[String, Int] , List[Tuple[String, Int]]]] = {
    // input path is in the form of an inverted index already
    val lines = sc.textFile(inputPath) // Each line is ((word, totalcount), postingslist)
    var i = 0
    var topNList = List[AnyRef]()
    for (i <- 0 until n){
      topNList = lines(i) :: topNList
    }
    topNList.reverse()
  }

  def searchForTerm(inputPath: String, outputPath: String, term: String): Tuple[String, Int, List[Tuple[String, Int]]] = {
    // input path is in the form of an inverted index already
    val lines = sc.textFile(inputPath) // Each line is ((word, totalcount), postingslist)
    lines.map{
      case ((w, t), l) => (w, t, l)
    }.filter(_._1 == term)
    lines
  }
}
