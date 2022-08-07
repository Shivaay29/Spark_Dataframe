import Dataframe2.readingdatadf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Dataframe2 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("Dataframe2").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  //val readingdata = sc.textFile("C:\\Users\\admin\\Desktop\\Shared_Folder\\10000_Sales_Records.csv")
  val headerdata = "Region string,Country string,Item_Type string,Sales_Channel string,Order_Priority string,Order_Date string,Order_ID int,Ship_Date string,Units_Sold int,Unit_Price float,Unit_Cost float,Total_Revenue float,Total_Cost float,Total_Profit float"

  val readingdatadf = spark.read
    .format("csv")
    .schema(headerdata)
    //.option("inferschema",true)
    .option("header",true)
    .option("path","C:\\Users\\admin\\Desktop\\Shared_Folder\\10000_Sales_Records.csv")
    .load()
  readingdatadf.show()
  readingdatadf.printSchema()
/*
  readingdatadf.write
    .format("csv")
    .option("header",true)
    .mode("overwrite")
    .option("path","C:\\Users\\admin\\Desktop\\Shared_Folder\\Save_Output\\output")
    .save()
  */

  readingdatadf.createOrReplaceTempView("orders")

  val mysqlquerie = spark.sql("Select * from orders limit 10")
  mysqlquerie.show()
  val mysqlquerie2 = spark.sql("select count (*) from orders")
  mysqlquerie2.show()

  println("Provide the number of partition :" +readingdatadf.rdd.getNumPartitions)
  val newpartitions = readingdatadf.repartition(4)
  println("Provide the number of partition after repartion :" +newpartitions.rdd.getNumPartitions)
/*
  newpartitions.write
    .format("csv")
    .mode("overwrite")
    .partitionBy("Region")
    .option("path","C:\\Users\\admin\\Desktop\\Shared_Folder\\Save_Output\\partitionoutput")
    .save()
 */
  newpartitions.write
    .format("csv")
    .mode("overwrite")
    .partitionBy("Region")
    .bucketBy(4,"Order_ID")
    .option("path","C:\\Users\\admin\\Desktop\\Shared_Folder\\Save_Output\\partitionbucketoutput")
    .saveAsTable("BucketTable")
  spark.stop()
}
