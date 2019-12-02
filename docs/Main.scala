import scala.io.StdIn._

object Main {
  def main (args: Array[String]): Unit = {
    /** Driver for main program here*/
    var continue = true
    /** TODO: POST/somehow create Inverted Inverted Index */
    /** TODO: Get :ok Response */
    while (continue == true){
      /** Get algorithm input from user*/
      println("Please enter one of the following algorithms to run: \n\t* topN \n\t* searchForTerm")
      val alg = readLine()
      if (alg == "topN"){
        println("Please enter a number")
        val n = readInt()
        /** TODO: POST/Run algorithm*/
        /** TODO: Get and print results*/
      } else if (alg == "searchForTerm") {
        println("please enter a term to search")
        val term = readLine()
        /** TODO: POST/Run algorithm*/
        /** TODO: Get and print results*/
      } else {
        println("Invalid")
        continue = false
      }
      println("would you like to continue searching?")
      var yn = readChar()
      yn match{
        case 'y' => continue = true
        case 'n' => continue = false
      }

    }
  }
}
