package sparktest;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.net.URISyntaxException;


public class TestSparkCon {

    public String resourcefile = this.getClass().getClassLoader().getResource("emp.csv").getPath();

    public static void main(String h[]) throws URISyntaxException {

        //TestSparkCon conf = new TestSparkCon();

        SparkSession spark = SparkSession
                .builder()
                //.master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        spark.conf().set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation",true);

        ClassLoader loader = TestSparkCon.class.getClassLoader();
        String empCsv = loader.getResource("emp.csv").getPath().replace("testngSpark-1.0-SNAPSHOT.jar!", "");
        String deptCsv = loader.getResource("dept.csv").getPath().replace("testngSpark-1.0-SNAPSHOT.jar!", "");

        Dataset<Row> empData = spark.read()
                .option("header", "true")
                .schema(getEmpSchema())
                .csv(empCsv);

        Dataset<Row> deptData = spark.read()
                .option("header", "true")
                .schema(getDeptSchema())
                .csv(deptCsv);

        Dataset<Emp> empDataset = empData.as(Encoders.bean(Emp.class));
        Dataset<Dept> deptDataset = deptData.as(Encoders.bean(Dept.class));
        empDataset.write().mode(SaveMode.Overwrite).saveAsTable("test_odm_team.empData");
        deptDataset.write().mode(SaveMode.Overwrite).saveAsTable("test_odm_team.deptData");
        // empData.printSchema();

    }

    public static StructType getEmpSchema() {

        StructField[] fields = {new StructField("emp_id", DataTypes.IntegerType, true, Metadata.empty())
                ,new StructField("emp_name", DataTypes.StringType, true, Metadata.empty())
                ,new StructField("dept_id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("salary", DataTypes.DoubleType, true, Metadata.empty())};

        return new StructType(fields);
    }

    public static StructType getDeptSchema() {
        StructField[] fields = {new StructField("dept_id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("dept_name", DataTypes.StringType, true, Metadata.empty())};

        return new StructType(fields);
    }
}
