import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;


public class TestSparkCon {

    public  static  void  main(String h[]){
        SparkSession spark=SparkSession
                .builder()
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> empData=spark.read()
                .option("header","true")
                .schema(getEmpSchema())
                .csv("src/main/resources/emp.csv");

        Dataset<Emp> empDataset=empData.as(Encoders.bean(Emp.class));

        empDataset.map((Emp a)->a.emp_id,Encoders.INT()).show();
        empData.printSchema();

    }

    public static StructType getEmpSchema(){

        StructField[] fields={  new StructField("emp_id", DataTypes.IntegerType, true,Metadata.empty())
        ,new StructField("dept_id", DataTypes.IntegerType, true,Metadata.empty()),
         new StructField("salary", DataTypes.DoubleType, true,Metadata.empty())};

        return  new StructType(fields);

    }
}
