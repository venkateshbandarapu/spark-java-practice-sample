import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import sparktest.Dept;
import sparktest.Emp;
import sparktest.TestSparkCon;

public class empTest {

    public static  void main(String h[]){
        SparkSession spark=SparkSession
                .builder()
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        spark.udf().register("empIdUDF",
                (Integer empID)-> "EMP-TEST-"+Integer.toString(empID)
                , DataTypes.StringType);


        Dataset<Row> empData = spark.read()
                .option("header", "true")
                .schema(TestSparkCon.getEmpSchema())
                .csv("src/main/resources/emp.csv");

        Dataset<Row> deptData = spark.read()
                .option("header", "true")
                .schema(TestSparkCon.getDeptSchema())
                .csv("src/main/resources/dept.csv");

        Dataset<Emp> empDataset = empData.as(Encoders.bean(Emp.class));
        Dataset<Dept> deptDataset = deptData.as(Encoders.bean(Dept.class));
        TestSparkCon.getMaxSalEmpByDept(empDataset,deptDataset).show();
       // empDataset.show();
        //deptDataset.show();
    }
}
