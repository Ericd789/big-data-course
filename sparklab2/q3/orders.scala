//Author: Eric Byjoo Collaborators: Jubrial Saigh
//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q3 {
    def main(args: Array[String]) = { // entry point to code
        val sc = getSC() // one function to get the sc variable
        val myrdd = getRDD(sc) // one function to get the rdd
        val custRDD = myrdd._1 // get custRDD from first element of tuple
        val orderRDD = myrdd._2  // get orderRDD from second element of tuple
        val cost = doOrders(custRDD,orderRDD) // one function to do the computation
        saveit(cost, "lab2q3") // save the rdd to home directory for gradescope
        runTest(sc)
        // runTest here to run before running against hdfs, runTest can also be called in the spark-shell
    }
    
    def getSC() = { // get the spark context variable
        val conf = new SparkConf().setAppName("Q2 customers")
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc: SparkContext) = { // get the big data rdd from the orders dataset
        val cust = sc.textFile("/datasets/orders/customers.csv")
        val order = sc.textFile("/datasets/orders/orders*")
        (cust, order)
    }

    def doOrders(customers: RDD[String], orders: RDD[String]): RDD[(String, Int)] = {
        // clean cust data
        val cust_data = customers.map(line => line.split("\t",5)) // each line has columns split by tabs
        val cust_filtered = cust_data.filter{line => !line.contains("Country")} // remove header row
        val cust_line = cust_filtered.map{line => (line(0),line(1))} // save customer id and country
        //clean order data
        val order_data = orders.map(line => line.split("\t",5)) // each line has columns split by tabs
        val order_filtered = order_data.filter{line => !line.contains("StockCode")} // remove header row
        val order_line = order_filtered.map{line => (line(4),line(2))} // save customerID and quantity
        // join data using 1 dependency wide transformation
        val cust_order = order_line.join(cust_line) //join country to quantity, will join based on matching customerID, this new tuples new format will be (custID,(quantity,country))
        //map & reduce
        val country_quant = cust_order.map(line => (line._2._2, line._2._1.toInt)) //remap so that value format is (country,quantity)
        val final_ans = country_quant.reduceByKey((x,y) => x + y) // reduce by key to get total quantity per location per customer, 2nd dependency wide transformation
        final_ans
    }

    def saveit(counts: RDD[(String, Int)], name: String) = { // simply saves the file
        counts.saveAsTextFile(name)
    }

    def getTestRDD(sc: SparkContext): (RDD[String], RDD[String]) = {
        val testdata = List("CustomerID\tCountry",
                            "12364\tBelgium",
                            "12365\tCyprus",
                            "12367\tDenmark",
                            "12370\tAustria",
                            "12371\tSwitzerland",
                            "12372\tDenmark")
        val custdata = sc.parallelize(testdata)
        val testdata1 = List("InvoiceNo\tStockCode\tQuantity\tInvoiceDate\tCustomerID",
                            "654321\t85123A\t6\t12/1/2010 8:26\t12364",
                            "654320\t71053\t6\t12/1/2010 8:26\t12364",
                            "654321\t84406B\t8\t12/1/2010 8:26\t12367")
        val orderdata = sc.parallelize(testdata1)
        (custdata,orderdata)
    }

    def runTest(sc: SparkContext) = {
        val testRDD = getTestRDD(sc) // get the small RDD
        val test_custRDD = testRDD._1 // get test custRDD from first element of tuple
        val test_orderRDD = testRDD._2 // get orderRDD from second element of tuple
        val results = doOrders(test_custRDD,test_orderRDD) // Perform computation on the function
        val get_arr = results.collect() // turns the elements of counts (all the words and their count) into an array
        results.collect().foreach(println) // prints each element in the array
    }


}

