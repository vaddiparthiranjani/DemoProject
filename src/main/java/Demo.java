import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;

public class Demo {
        public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

        Dataset<Row> stu = spark
                .read()
                .option("infer schema",true)
                .option("header",true)
                .csv("C:\\Users\\dell\\Documents\\stu.csv");
        //stu.show();

        //stu.select("course_id","course_title","num_lectures","level").show();
        stu.filter("Rating == '0.96'").show();
        //df.printSchema();

    }
}
