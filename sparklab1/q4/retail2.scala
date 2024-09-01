//Author: Eric Byjoo Collaborators: Jubrial Saigh
//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q4 {
    def main(args: Array[String]) = {  // this is the entry point to our code
        val sc = getSC() // one function to get the sc variable
        val myrdd = getRDD(sc) // one function to get the rdd
        val cost = doRetail(myrdd) // one function to do the computation
        saveit(cost, "spark4output") // save the rdd to home directory for gradescope
        runTest(sc) 
        //runTest here to run before running against hdfs, runTest can also be called in the spark-shell
    }

    def getSC() = { // get the spark context variable
        val conf = new SparkConf().setAppName("question4")
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc:SparkContext) = { // get the big data rdd from the retailtab dataset
        sc.textFile("/datasets/retailtab")
    }

    def doRetail(input: RDD[String]): RDD[(String, (Int, Double))] = {
        val data = input.map(line => line.split("\t")) // mapping each line to be split by tabs
        val orders = data.filter{line => !line.contains("Country")} // Filter for all lines not containing "country". Removes headers
        val line_data = orders.map(line => (line(0), line(3),line(5))) // save the lines needed, InvoiceNo, Quantity, and UnitPrice
        val unique_item = 1
        val line_costs = line_data.map(line => (line._1, (unique_item,line._2.toDouble*line._3.toDouble))) // get kv with invoiceNo as key and value as a tuple consisting of the unique item assignment and unique cost (quanitity * unitprice)
        val total_cost = line_costs.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))  // compute total number of unique items and combine all the same orders, will now have each order and total cost of order
        total_cost
    }
    
    def saveit(counts: RDD[(String, (Int,Double))], name: String) = {  // simply saves the file
        counts.saveAsTextFile(name)
    }

    def runTest(sc: SparkContext) = {
        val testRDD = getTestRDD(sc) // get the small RDD
        val counts = doRetail(testRDD) // Perform computation on the function
        val get_arr = counts.collect() // turns the elements of counts (all the words and their count) into an array
        get_arr.foreach{x => println(x)} // prints each element in the array
    }

    def getTestRDD(sc: SparkContext): RDD[String] = { // small testing RDD from examples
        val myorders = List("InvoiceNo	StockCode	Description	Quantity	InvoiceDate	UnitPrice	CustomerID	Country",
                           "111111	85123A	BLACK HOODIE	6	12/1/2023 8:26	10.55	10000	United States",
                           "111111	71053	WHITE HOODIE	6	12/1/2023 8:26	10.39	10000	United States")
        sc.parallelize(myorders, 4) // correct output will be ('111111',125.64)
    }


}
