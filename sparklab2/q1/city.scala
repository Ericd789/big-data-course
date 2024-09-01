//Author: Eric Byjoo Collaborators: Jubrial Saigh
//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q1 {
    def main(args: Array[String]) = {  // entry point to code
        val sc = getSC() // one function to get the sc variable
        val myrdd = getRDD(sc) // one function to get the rdd
        val cost = doCity(myrdd) // one function to do the computation
        saveit(cost, "lab2q1") // save the rdd to home directory for gradescope
        runTest(sc)
        //runTest here to run before running against hdfs, runTest can also be called in the spark-shell
    }

    def getSC() = {  // get the spark context variable
        val conf = new SparkConf().setAppName("Q1 cities")
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc: SparkContext) = { // get the big data rdd from the retailtab dataset
        sc.textFile("/datasets/cities")
    }

    def doCity(input: RDD[String]): RDD[(String, (Int, Int, Int))] = {
        val data = input.map(line => line.split("\t")) // each line has columns split by tabs
        val data_filtered = data.filter{line => !line.contains("Population")} // remove header line
        val final_data = data_filtered.filter{line => line.length == 7}  // filter out rows with bad data (wrong number of columns)
        val line_data = final_data.map{line => (line(1),line(4),line(5).split(" ").length)} // save state abrev, population, number of zip codes separated by spaces
        val unique_item = 1
        val save_by_key = line_data.map{line => (line._1,(unique_item,line._2.toInt,line._3))} // remap the data so that key is state abrev, value is a tuple containing a 1 for number of cities, population as a int and the number of zip codes
        val total = save_by_key.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2, math.max(x._3, y._3))) // one wide dependency transformation to find number of cities, total population and max zip codes
        total
    }
    def saveit(counts: RDD[(String, (Int, Int, Int))], name: String) = { // simply saves the file
        counts.saveAsTextFile(name)
    }

    def runTest(sc: SparkContext) = {
        val testRDD = getTestRDD(sc) // get the small RDD
        val results = doCity(testRDD) // Perform computation on the function
        val get_arr = results.collect() // turns the elements of counts (all the words and their count) into an array
        get_arr.foreach{x => println(x)} // prints each element in the array
    }

    def getTestRDD(sc: SparkContext): RDD[String] = { //small test rdd
        val testData = List("City\tState Abbreviation\tState\tCounty\tPopulation\tZip Codes(space separated)\tID",
                            "Staten Island\tNY\tNew York\tBronx\t500000\t11305 15499 16648\t100863",
                            "Nashville\tTN\tTennessee\tDavidson\t692000\t37011 37013 37015 37024\t154200")
        sc.parallelize(testData,4)
    }
}
