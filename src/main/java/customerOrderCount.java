import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class customerOrderCount {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

        Dataset<Row> orders = spark.read()
                .option("infer schema", true)
                .option("header", true)
                .csv("C:\\DemoProject\\src\\main\\Data\\retail_db\\orders\\part-00000");

        //orders.show(10);
        orders.groupBy("order_status").count().show();

        Dataset<Row> customers = spark.read()
                .option("infer schema", true)
                .option("header", true)
                .csv("C:\\DemoProject\\src\\main\\Data\\retail_db\\customers\\part-00000");

        //customers.show(10);

      customers.groupBy("customer_city").count().orderBy("customer_city").show();

      // Dataset<Row> order= spark.sql("select count(*) from orders ");

      // order.show();

        /* SELECT
        c.customer_id,
                c.customer_fname customer_first_name,
                c.customer_lname customer_last_name,
                count(o.order_customer_id) customer_order_count
        FROM orders o JOIN customers c
        ON o.order_customer_id = c.customer_id
        WHERE date_format(o.order_date ,'yyyy-MM') = '2014-01'
        GROUP BY c.customer_id,c.customer_fname,c.customer_lname
            ORDER BY customer_id,customer_order_count desc").show() */



    }
}