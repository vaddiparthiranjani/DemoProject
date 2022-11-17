import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;

public class aggFunctionDemo {
    public static void main(String[] args){

    SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        Dataset<Row> orders = spark.read()
                .option("infer schema", true)
                .option("header", true)
                .csv("C:\\DemoProject\\src\\main\\Data\\retail_db\\orders\\part-00000");
       // orders.show();

        // orders.printSchema();

        // orders.select("order_id", "order_date","order_customer_id").show();

        // orders.select(functions.col("order_id"),functions.lower(functions.col("order_status"))
        // .as("order_status")).show();

        /* orders.select(functions.col("order_id"),
                functions.substring(functions.col("order_date"), 0,7)
                        .as("order_date"),functions.col("order_status")).show(); */

        //orders.select(functions.countDistinct(functions.col("order_status"))).show();
        //orders.groupBy(functions.col("order_status")).count().show();
        orders.select(functions.col("*"),functions.date_format(functions.col("order_date"),"yyyyMM").as("order_month")).show();




}}
