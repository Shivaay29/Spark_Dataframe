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
    .option("header",false)
    .option("path","C:\\Users\\admin\\Desktop\\Shared_Folder\\10000_Sales_Records.csv")
    .load()
  readingdatadf.show()
  readingdatadf.printSchema()

  readingdatadf.write
    .format("csv")
    .option("header",true)
    .mode("overwrite")
    .option("path","C:\\Users\\admin\\Desktop\\Shared_Folder\\Save_Output\\output")
    .save()

}
