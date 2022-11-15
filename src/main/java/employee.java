import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class employee{
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();
        Dataset<Row> employeeDF= spark.read()
                .option("infer schema", true)
                .option("header", true)
                .csv("C:\\DemoProject\\src\\main\\Data\\retail_db\\employee");
        Dataset<Row> transformedDF = employeeDF.as("emp").join(employeeDF.as("manager")
                        , col("emp.ManagerId").equalTo(col("manager.EmpId")))
                .selectExpr("emp.EmpId", "emp.ManagerId", "emp.EmpName", "manager.EmpName as ManagerName");
        transformedDF.show();

    }
}