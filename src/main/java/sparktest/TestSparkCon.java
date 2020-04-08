package sparktest;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.*;
import java.net.URISyntaxException;
import org.apache.spark.sql.functions.*;
import scala.Int;


public class TestSparkCon {

    public static void main(String h[]) {

        SparkSession spark = SparkSession
                .builder()
                .enableHiveSupport()
                .getOrCreate();


        spark.udf().register("empIdUDF",
                (Integer empID)-> "EMP-TEST-"+Integer.toString(empID)
                ,DataTypes.StringType);


        spark.conf().set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation",true);

        Dataset<Row> empData=spark.table("test_odm_team.empData");
        Dataset<Row> deptData=spark.table("test_odm_team.deptData");

        Dataset<Emp> empDataset = empData.as(Encoders.bean(Emp.class));
        Dataset<Dept> deptDataset = deptData.as(Encoders.bean(Dept.class));

        getMaxSalEmpByDept(empDataset,deptDataset)
                .write().mode(SaveMode.Overwrite).saveAsTable("test_odm_team.result_max_empsal_by_dept");

    }

    public static Dataset<Row> getMaxSalEmpByDept(Dataset<Emp> empData,Dataset<Dept> deptData){

        Dataset<Row> rankedEmpSalByDept=empData.withColumn("ranking",
                functions.row_number().over(Window.partitionBy(empData.col("dept_id")).orderBy(functions.desc("salary"))));

        Dataset<Row> maxEmpSalByDept=rankedEmpSalByDept
                .filter(rankedEmpSalByDept.col("ranking").equalTo("1"))
                .drop("ranking");

        Dataset<Row> maxSalEmpByDept=maxEmpSalByDept.join(deptData,
                maxEmpSalByDept.col("dept_id").equalTo(deptData.col("dept_id")),"left_outer")
                .withColumn("derived_emp_id",functions.callUDF("empIdUDF",maxEmpSalByDept.col("emp_id")));

        Dataset<Row> maxSalEmpByDeptData=maxSalEmpByDept.select(maxSalEmpByDept.col("derived_emp_id"),maxEmpSalByDept.col("emp_id"),maxEmpSalByDept.col("emp_name")
                ,deptData.col("dept_name"),maxEmpSalByDept.col("salary"));

       return maxSalEmpByDeptData;
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
