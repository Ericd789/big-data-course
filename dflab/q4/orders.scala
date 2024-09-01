//Author: Eric Byjoo Collaborator: Jubrial Saigh
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q4 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        // do not change this function
        val spark = getSparkSession() // get spark context variable
        import spark.implicits._
        val (c, o, i) = getDF(spark) // get data thats been stored in our dataframe
        val counts = doOrders(c,o,i) // perform computation
        saveit(counts, "dflabq4")  // save output to HDFS directory
    }


    def doOrders(customers: DataFrame, orders: DataFrame, items: DataFrame): DataFrame = {
        val cust_filled = customers.na.fill("blank", Seq("CustomerID")) // fix one customer has bad ID
        val order_filled = orders.na.fill("blank", Seq("CustomerID")) // fix one customer has bad ID

        val clean_customers = cust_filled.where(col("Country").isNotNull) // remove bad data
        val clean_orders = order_filled.where(col("StockCode").isNotNull) // remove bad data
        val clean_items = items.where(col("StockCode").isNotNull) // remove bad data

        val order_cust = clean_orders.join(clean_customers, "CustomerID") // join customer on orders by customerID
        val order_item = order_cust.join(clean_items, "StockCode") // join item on customer and order by StockCode 

        val cost = order_item.withColumn("total", col("Quantity") * col("UnitPrice")) // make a new column with total cost
        val data_grouped = cost.groupBy("Country") // regroup the rows by country 
        val total = data_grouped
         .agg(
            sum("total").as("total")
            ) // aggregate just the total cost

        total 

    }

    def getDF(spark: SparkSession): (DataFrame,DataFrame,DataFrame) = {
        val customerSchema = new StructType()
         .add("CustomerID", StringType, true)
         .add("Country", StringType, true) // schema is based on the header of customers datasets
        
        val orderSchema = new StructType()
         .add("InvoiceNo", StringType, true)
         .add("StockCode", StringType, true)
         .add("Quantity", IntegerType, true) 
         .add("InvoiceDate", StringType, true) 
         .add("CustomerID", StringType, true) // schema is based on the header of orders dataset

        val itemSchema = new StructType()
         .add("StockCode", StringType, true)
         .add("Description", StringType, true) 
         .add("UnitPrice", DoubleType, true) // schema is based on the header of items dataset 
                
        val cust_csv = "/datasets/orders/customers.csv"
        val order_csv = "/datasets/orders/orders*"
        val item_csv = "/datasets/orders/items.csv" // paths to the data sets 
        
        val custdfcsv = spark.read.format("csv").schema(customerSchema)
         .option("header", true)
         .option("delimiter", "\t") 
         .load(cust_csv) // load data into dataframe, letting it know it has a header and each row is split by tabs

        val orderdfcsv = spark.read.format("csv").schema(orderSchema)
         .option("header", true)
         .option("delimiter", "\t") 
         .load(order_csv) // load data into dataframe, letting it know it has a header and each row is split by tabs
        
        val itemdfcsv = spark.read.format("csv").schema(itemSchema)
         .option("header", true)
         .option("delimiter", "\t") 
         .load(item_csv) // load data into dataframe, letting it know it has a header and each row is split by tabs

        (custdfcsv,orderdfcsv,itemdfcsv)
    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        spark
    }

    def getTestDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        import spark.implicits._ // so you can use .toDF
        val custtest_DF = Seq(("12364","Belgium"),
                            ("12365","Cyprus"),
                            ("12367","Denmark"),
                            ("12370","Austria"),
                            ("12371","Switzerland"),
                            ("12372","Denmark")
        ).toDF("CustomerID","Country")

        val ordertest_DF = Seq(("654321","85123A",6,"12/1/2010 8:26","12364"),
                            ("654320","71053",6,"12/1/2010 8:26","12364"),
                            ("654321","84406B",8,"12/1/2010 8:26","12367")
        ).toDF("InvoiceNo","StockCode","Quantity","InvoiceDate","CustomerID")

        val itemtest_DF = Seq(("85123","birds",1.69),
                            ("721053","dogs",1.69),
                            ("84406B","cats",1.69)
        ).toDF("StockCode","Description","UnitPrice")
       
        (custtest_DF,ordertest_DF,itemtest_DF)   
    }

    def runTest(spark: SparkSession) = {
        import spark.implicits._
        val (c, o, i) = getDF(spark) // get test DF
        val results = doOrders(c, o, i) // perform computation
        results.collect().foreach{x => println(x)} // print each result
    }

    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)
    }
}

