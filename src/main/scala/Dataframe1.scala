import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Dataframe1 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("Dataframe1").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  val orderlist = List((51320,"2022-08-07",1,"Complete"),
    (51321,"2022-08-07",1,"Complete"),
      (51322,"2022-08-07",4,"Complete"),
        (51323,"2022-08-07",2,"Pending"),
          (51324,"2022-08-07",3,"Pending"),
            (51324,"2022-08-07",4,"Pending"),
              (51325,"2022-08-07",2,"Complete"),
                (51326,"2022-08-07",3,"Complete"))

  import spark.implicits._
  val orderrdd = sc.parallelize(orderlist)
  val orderrddtoDF = orderrdd.toDF()
  orderrddtoDF.show()

  val orderdF = spark.createDataFrame(orderlist).toDF("order_id","order_date","customer_Id","order_Status")
  orderdF.show()
  val finalorderdF = orderdF.withColumn("order_date",unix_timestamp(col("order_date").cast("date")))
    .dropDuplicates("order_id")
    .withColumn("New_ID",monotonically_increasing_id())
    .sort("New_ID")
  finalorderdF.show()

spark.stop()
}
