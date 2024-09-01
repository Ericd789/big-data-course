//Author: Eric Byjoo Collaborators: Jubrial Saigh
//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q2 {
    def main(args: Array[String]) = { // entry point to code
        val sc = getSC() // one function to get the sc variable
        val myrdd = getRDD(sc) // one function to get the rdd
        val cost = doOrders(myrdd) // one function to do the computation
        saveit(cost, "lab2q2") // save the rdd to home directory for gradescope
        runTest(sc)
        // runTest here to run before running against hdfs, runTest can also be called in the spark-shell
    }

    def getSC() = { // get the spark context variable
        val conf = new SparkConf().setAppName("Q2 customers")
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc: SparkContext) = { // get the big data rdd from the orders dataset
        sc.textFile("/datasets/orders/customers.csv")
    }
    
    def doOrders(customers: RDD[String]): RDD[(String, Int)] = {
        val data = customers.map(line => line.split("\t")) // each line has columns split by tabs
        val data_filtered = data.filter{line => !line.contains("Country")} // remove header row
        val unique_val = 1
        val line_data = data_filtered.map{line => (line(1),unique_val)} // save the country assigned with a value of 1
        val total = line_data.reduceByKey((x,y) => (x + y)) // one wide dependency transformation to get the total amount of appearances of the country which is the number of customers
        total
    }

    def saveit(counts: RDD[(String, Int)], name: String) = { // simply saves the file
        counts.saveAsTextFile(name)
    }

    def getTestRDD(sc: SparkContext): RDD[String] = {
        val testData = List("CustomerID\tCountry",
                            "12364\tBelgium",
                            "12365\tCyprus",
                            "12367\tDenmark",
                            "12370\tAustria",
                            "12371\tSwitzerland",
                            "12372\tDenmark")
        sc.parallelize(testData)
    }
    def runTest(sc: SparkContext) = {
        val testRDD = getTestRDD(sc) // get the small RDD
        val results = doOrders(testRDD) // Perform computation on the function
        val get_arr = results.collect() // turns the elements of counts (all the words and their count) into an array
        results.collect().foreach(println) // prints each element in the array
    }


}

