import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
//import org.apache.spark.sql.functions.*;

public class wordCount {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        Dataset<Row> df = spark.read().text("C://DemoProject//src//main//Data//wordCount.txt");
        df.show(false);

    }
}
