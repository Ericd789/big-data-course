//Author: Eric Byjoo Collaborator: Jubrial Saigh
import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q1 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        // do not change this function
        val spark = getSparkSession() // get spark context variable
        import spark.implicits._ 
        val mydf = getDF(spark) // get data thats been stored in our dataframe
        val counts = doCity(mydf) // perform computation
        saveit(counts, "dflabq1")  // save output to HDFS directory
    }

    def registerZipCounter(spark: SparkSession) = {
        val zipCounter = udf({x: String => Option(x) match {case Some(y) => y.split(" ").size; case None => 0}})
        spark.udf.register("zipCounter", zipCounter) // registers udf with the spark session
    }

    def doCity(input: DataFrame): DataFrame = {
        val clean_df = input.where(col("Population").isNotNull) // remove bad data
        
        val countzipcodes = clean_df.withColumn("Zipcount", callUDF("zipCounter", col("Zip Codes"))) // transform zipcodes column to a have count of zipcodes
        
        val group_SA = countzipcodes.groupBy("State Abbreviation")  // Similar to having a key as SA, data is organized by SA
        
        val aggregate_data = group_SA.agg(
            count("City").as("citycount"), 
            sum("Population").as("totalpop"),
            max("Zipcount").as("maxzip")
            ) // aggregate needed data using bulit in count, sum, and max on cities, population, and count of zipcodes
        
        aggregate_data
    }

    def getDF(spark: SparkSession): DataFrame = {
        val mySchema = new StructType()
         .add("City", StringType, true)
         .add("State Abbreviation", StringType, true)
         .add("State", StringType, true)
         .add("County", StringType, true)
         .add("Population", IntegerType, true)
         .add("Zip Codes", StringType, true)
         .add("ID", IntegerType, true) // schema is based on the header of dataset 
        
        val data_csv = "/datasets/cities" // path to dataset
        
        val dfcsv = spark.read.format("csv").schema(mySchema)
         .option("header", true)
         .option("delimiter", "\t") 
         .load(data_csv) // load data into dataframe, letting it know it has a header and each row is split by tabs

        dfcsv
    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark) // tells the spark session about the UDF
        spark
    }

    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._ // so you can use .toDF
        val testDF = Seq(("Staten Island","NY","New York","Bronx",500000,"11305 15499 16648", 100863),
                         ("Nashville","TN","Tennessee","Davidson",692000, "37011 37013 37015 37024", 154200)
        ).toDF("City", "State Abbreviation", "State", "County", "Population", "Zip Codes", "ID")

        testDF
    }

    def runTest(spark: SparkSession) = {
        import spark.implicits._
        val testDF = getTestDF(spark)  // get test dataframe
        val results = doCity(testDF)  // perform computation
        results.collect().foreach{x => println(x)}  // print each result
    }

    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)
    }

}
