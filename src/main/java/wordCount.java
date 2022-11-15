import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.*;
//import org.apache.spark.sql.functions.*;

public class wordCount {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

        Dataset<Row> linesDF = spark.read().text("C://DemoProject//src//main//Data//wordCount" );

        linesDF.printSchema();
        linesDF.show();

         Dataset<Row> wordsDF = linesDF.select(functions.explode(functions.split(functions.col("linesDF")," "))).
                as("word").groupBy("word").agg(functions.count(functions.lit(1))
                        .as("wordCount"));

        wordsDF.show();

    }
}
