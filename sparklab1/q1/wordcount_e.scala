//Author: Eric Byjoo Collaborators: Jubrial Saigh Jack Bresney
//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q1 {
    def main(args: Array[String]) = {  // Main function runs all of the code
        val sc = getSC() // one function to get the sc variable
        val myrdd = getRDD(sc) // one function to get the rdd
        val counts = doWordCount(myrdd) // one function to do the computation
        saveit(counts, "spark1output") // save the rdd to home directory for gradescope
        runTest(sc)
        //runTest here to run before running against hdfs, runTest can also be called in the spark-shell
    }

    def getSC() = { // get the spark context variable
        val conf = new SparkConf().setAppName("question1")
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc:SparkContext) = { // get the big data rdd from war and piece dataset
        sc.textFile("/datasets/wap")
    }

    def doWordCount(input: RDD[String]): RDD[(String, Int)] = { // function computing all the words with the letter e in them
        val words = input.flatMap(_.split("\\s+")) // split string by spaces
        val remove_e = words.filter(word => word.contains("e")) // filter out words containing e
        val kv = remove_e.map(word => (word,1)) // map each word to be a tuple containing the word and a value of 1 for appearing once
        val counts = kv.reduceByKey((x,y) => x+y) // reduces by keeping count of how many times a word has appeared 
        counts
    }

    def saveit(counts: RDD[(String, Int)], name: String) = { // simply saves the file
        counts.saveAsTextFile(name)
    }


    def runTest(sc: SparkContext) = {
        val testRDD = getTestRDD(sc) // get the small RDD
        val counts = doWordCount(testRDD) // Perform computation on the function
        val get_arr = counts.collect() // turns the elements of counts (all the words and their count) into an array
        get_arr.foreach{x => println(x)} // prints each element in the array
    }

    def getTestRDD(sc: SparkContext): RDD[String] = { // small testing RDD from examples
        val mylines = List("it was the best of times, wasn't it",
                            "it was the worst of times of all time")
        sc.parallelize(mylines, 3)
    }


}
 