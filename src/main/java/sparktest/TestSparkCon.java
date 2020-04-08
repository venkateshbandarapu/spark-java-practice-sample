package sparktest;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.*;
import java.net.URISyntaxException;
import org.apache.spark.sql.functions.*;


public class TestSparkCon {

    public String resourcefile = this.getClass().getClassLoader().getResource("emp.csv").getPath();

    public static void main(String h[]) throws URISyntaxException {

        //TestSparkCon conf = new TestSparkCon();

        SparkSession spark = SparkSession
                .builder()
                .enableHiveSupport()
                .getOrCreate();

        spark.conf().set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation",true);

        Dataset<Row> empData=spark.table("test_odm_team.empData");
        Dataset<Row> deptData=spark.table("test_odm_team.deptData");

        Dataset<Emp> empDataset = empData.as(Encoders.bean(Emp.class));
        Dataset<Dept> deptDataset = deptData.as(Encoders.bean(Dept.class));

        getMaxSalEmpByDept(empDataset,deptDataset)
                .write().mode(SaveMode.Overwrite).saveAsTable("test_odm_team.result_max_empsal_by_dept");

    }
    public static Dataset<Row> getMaxSalEmpByDept(Dataset<Emp> empData,Dataset<Dept> deptData){

        Dataset<Row> rankedEmpByDept=empData.withColumn("ranking",
                functions.row_number().over(Window.partitionBy(empData.col("dept_id")).orderBy(functions.desc("salary"))));

        Dataset<Row> groupEmpSalByDept=rankedEmpByDept
                .filter(rankedEmpByDept.col("ranking").equalTo("1"))
                .drop("ranking");

        Dataset<Row> maxSalEmpByDept=groupEmpSalByDept.join(deptData,
                groupEmpSalByDept.col("dept_id").equalTo(deptData.col("dept_id")),"left_outer")
                .select(groupEmpSalByDept.col("emp_id"),groupEmpSalByDept.col("emp_name"),deptData.col("dept_name"),groupEmpSalByDept.col("salary"));

       return maxSalEmpByDept;
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
