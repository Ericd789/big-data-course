//Author: Eric Byjoo Collaborators: Jubrial Saigh
//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object Q4 {
    def main(args: Array[String]) = { // entry point to code
        val sc = getSC() // one function to get the sc variable
        val myrdd = getRDD(sc) // one function to get the rdd
        val custRDD = myrdd._1 // get custRDD from first element of tuple
        val orderRDD = myrdd._2  // get orderRDD from second element of tuple
        val itemRDD = myrdd._3 // get itemRDD from third element of tuple
        val cost = doOrders(custRDD,orderRDD,itemRDD) // one function to do the computation
        saveit(cost, "lab2q3") // save the rdd to home directory for gradescope
        runTest(sc)
        // runTest here to run before running against hdfs, runTest can also be called in the spark-shell
    }

    def getSC() = { // get the spark context variable
        val conf = new SparkConf().setAppName("Q3")
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc: SparkContext) = { // get the big data rdd from the orders dataset
        val cust = sc.textFile("/datasets/orders/customers.csv")
        val order = sc.textFile("/datasets/orders/orders*")
        val item = sc.textFile("/datasets/order/items.csv")
        (cust, order,item)
    }

    def doOrders(customers: RDD[String], orders: RDD[String], items: RDD[String]): RDD[(String, Double)] = {
        // clean cust data
        val cust_data = customers.map(line => line.split("\t",5)) // each line has columns split by tabs
        val cust_filtered = cust_data.filter{line => !line.contains("Country")} // remove header row
        val cust_line = cust_filtered.map{line => (line(0),line(1))} // save customer id and country

        //clean order data
        val order_data = orders.map(line => line.split("\t",5)) // each line has columns split by tabs
        val order_filtered = order_data.filter{line => !line.contains("StockCode")} // remove header row
        val order_line = order_filtered.map{line => (line(4),(line(1),line(2)))} // save customerID and tuple with stock code and quantity

        ////clean item data
        val item_data = items.map(line => line.split("\t",5)) // each line has columns split by tabs
        val item_filtered = item_data.filter{line => !line.contains("StockCode")} // remove header row
        val item_line = item_filtered.map{line => (line(0),line(2))} // save stock code and unitprice

        //join customers onto order and then join item
        val cust_order = order_line.join(cust_line) // joined by matching custID, value structure is ((stock,quantity),country)
        val cust_order_remap = cust_order.map {line => (line._2._1._1, (line._2._2, line._2._1._2))} // remap value to be stock, (country, quantity)
        val all_items = cust_order_remap.join(item_line) // join item on cust&order based on stock code, value structure is (stock, ((country, quant), price))

        //final remap and reduce
        val cost = all_items.map { line => (line._2._1._1, line._2._1._2.toDouble * line._2._2.toDouble)} // remap to country, price
        val total_cost = cost.reduceByKey((x,y) => x + y) //reduce by country leading to total spent
        total_cost
    }

    def saveit(counts: RDD[(String, Double)], name: String) = { // simply saves the file
        counts.saveAsTextFile(name)
    }

    def getTestRDD(sc: SparkContext): (RDD[String], RDD[String], RDD[String]) = {
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
        val testdata2 = List("StockCode\tDescription\tUnitPrice",
                            "85123\tbirds\t1.69",
                            "721053\tdogs\t1.69",
                            "84406B\tcats\t1.69")
        val itemdata = sc.parallelize(testdata2)
        (custdata,orderdata,itemdata)
    }

    def runTest(sc: SparkContext) = {
        val testRDD = getTestRDD(sc) // get the small RDD
        val test_custRDD = testRDD._1 // get test custRDD from first element of tuple
        val test_orderRDD = testRDD._2 // get orderRDD from second element of tuple
        val test_itemRDD = testRDD._3 //get itemRDD from third element of tuple
        val results = doOrders(test_custRDD,test_orderRDD,test_itemRDD) // Perform computation on the function
        val get_arr = results.collect() // turns the elements of counts (all the words and their count) into an array
        results.collect().foreach(println) // prints each element in the array
    }


}

