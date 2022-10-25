import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;

public class Demo {
    //private static final String CSV_URL = "C:\\Users\\dell\\Documents\\stu.csv";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

        Dataset<Row> DF1 = spark
                .read()
                .option("header", true)
                .csv("C:\\Users\\dell\\Documents\\stu.csv");
        // DF1.show();
        DF1.select("course_id","course_title","num_lectures").show();
        // DF1.printSchema();
    }
}
