package sparktest;
import org.apache.hadoop.hdfs.protocol.SnapshotInfo;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.functions.*;

import lombok.Generated;
import scala.Tuple2;

public class Main {

    @Generated
    public static void main(String h[]) {

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        spark.udf().register("empIdUDF",
                (Integer empID)-> "EMP-TEST-"+Integer.toString(empID)
                ,DataTypes.StringType);

        spark.conf().set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation",true);

        Dataset<Row> empData=spark.table("test_odm_team.empData");
        Dataset<Row> deptData=spark.table("test_odm_team.deptData");

        Dataset<Emp> empDataset=empData
                .na().fill(0,new String[]{"dept_id"}).as(Encoders.bean(Emp.class));

        Dataset<Dept> deptDataset = deptData.as(Encoders.bean(Dept.class));

        getMaxSalEmpByDept(empDataset,deptDataset)
                .write().mode(SaveMode.Overwrite).saveAsTable("test_odm_team.result_max_empsal_by_dept");

        getTotalSalByDept(empDataset,deptDataset)
                .write().mode(SaveMode.Overwrite).saveAsTable("test_odm_team.result_total_empsal_by_dept");

    }

    /*
       To get the sum of employees salary for each department
     */
    public static Dataset<Row> getTotalSalByDept(Dataset<Emp> empData,Dataset<Dept> deptData){

       Dataset<Row> totalSalByDept=empData.groupBy(empData.col("dept_id")).agg(functions.sum("salary").as("total_salary"));

       Dataset<Row> totalSalByDeptData=totalSalByDept.join(deptData,totalSalByDept.col("dept_id").equalTo(deptData.col("dept_id")),"left_outer");

       return  totalSalByDeptData.select(totalSalByDept.col("dept_id"),deptData.col("dept_name"),totalSalByDept.col("total_salary"));

    }
    /*
        To get the highest employee salary for each department
     */

    public static Dataset<Row> getMaxSalEmpByDept(Dataset<Emp> empData,Dataset<Dept> deptData){

        Dataset<Row> rankedEmpSalByDept=empData.withColumn("ranking",
                functions.row_number().over(Window.partitionBy(empData.col("dept_id")).orderBy(functions.desc("salary"))));

        Dataset<Row> maxEmpSalByDept=rankedEmpSalByDept
                .filter(rankedEmpSalByDept.col("ranking").equalTo("1"))
                .drop("ranking");

        Dataset<Row> maxEmpSalByNullDept=maxEmpSalByDept
                .filter(maxEmpSalByDept.col("dept_id").equalTo("0"));

        Dataset<Row> maxEmpSalByValidDept=maxEmpSalByDept.except(maxEmpSalByNullDept);

        Dataset<Row> maxSalEmpDataByDept=maxEmpSalByValidDept.join(deptData,
                maxEmpSalByValidDept.col("dept_id").equalTo(deptData.col("dept_id")),"left_outer");

        Dataset<Row> maxSalEmpByValidDeptData=maxSalEmpDataByDept
                .select(maxSalEmpDataByDept.col("emp_id"),maxSalEmpDataByDept.col("emp_name"),maxEmpSalByValidDept.col("dept_id")
                ,deptData.col("dept_name"),maxSalEmpDataByDept.col("salary"));

        Dataset<Row> maxEmpSalByNullDeptData=maxEmpSalByNullDept
                .withColumn("dept_name",functions.lit("Not Available"));

        Dataset<Row> maxEmpSalByDeptData=maxSalEmpByValidDeptData
                .union(maxEmpSalByNullDeptData.selectExpr(maxSalEmpByValidDeptData.columns()));

        Dataset<Row> derivedEmpData=maxEmpSalByDeptData
                .withColumn("derived_emp_id",functions.callUDF("empIdUDF",rankedEmpSalByDept.col("emp_id")));

       return derivedEmpData;
    }


    /*
     To get the highest employee salary for each department using reduce groups
  */
    @Generated
    public static  Dataset<Emp> getMaxSalEmpByDept2(Dataset<Emp> empData,Dataset<Dept> deptData){

       Dataset<Emp> maxSalEmpByDeptData= empData.groupByKey((MapFunction<Emp,Integer>) row -> row.dept_id ,Encoders.INT())
        .reduceGroups((ReduceFunction<Emp>) (e1, e2)->{
            Emp e;
            if (e1.salary > e2.salary){ e=e1;
            }
            else { e=e2;
            }
            return e;

        }).map((MapFunction<Tuple2<Integer, Emp>, Emp>) a->a._2,Encoders.bean(Emp.class));

       return maxSalEmpByDeptData;

    }
    public static Dataset<Row> collectEmpSalariesByDept(Dataset<Emp> empData,Dataset<Dept> deptData){
        Dataset<Row> collectedEmpSal=empData
                .groupBy(empData.col("dept_id")).agg(functions.collect_list("salary").as("collect_salary"));

        Dataset<Row> collectedEmpSalWithDept=collectedEmpSal
                .join(deptData,empData.col("dept_id").equalTo(deptData.col("dept_id")),"left_outer")
                .select(collectedEmpSal.col("dept_id"),deptData.col("dept_name"),collectedEmpSal.col("collect_salary"));
      return collectedEmpSalWithDept;

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
