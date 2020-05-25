import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.mutable.Seq;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;

public class AccumTest {
    public static void main(String a[]){
SparkSession spark= SparkSession
                .builder()
                .master("local[*]")
                .getOrCreate();

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        Dataset<Row> empData = spark.read()
                .option("header", "true")
                .schema(getEmpSchema())
                .csv("src/main/resources/emp_joindate.csv");


        registerUDF(spark);

       Dataset<Row> groupedData= empData.orderBy(functions.asc("join_date")).groupBy("emp_id")
               .agg(functions.collect_list(empData.col("join_date")).as("list_join_dates"));

        groupedData.printSchema();
        Dataset<Row> ValidgroupedData= groupedData
                .withColumn("valid_join_dates",functions.callUDF("empJoinDateUdf",groupedData.col("list_join_dates")));
        Dataset<Row>empDataWithSvcDt=ValidgroupedData.withColumn("svc_start_dt",functions.explode(ValidgroupedData.col("valid_join_dates")))
                .drop("list_join_dates","valid_join_dates").dropDuplicates();
        empDataWithSvcDt.printSchema();
        empDataWithSvcDt.show(false);


    }

    public static void registerUDF(SparkSession session){
        session.udf().register("empJoinDateUdf", (Seq<Timestamp> join_dates)->{
                    Timestamp updated_TS=null;
                    ArrayList<Timestamp> validJoinDates=new ArrayList<>();
                    for( int i=0;i<join_dates.length();i++){
                        Timestamp joinDT=join_dates.apply(i);
                        if(updated_TS==null){
                            updated_TS=joinDT;
                            if(join_dates.length()==1){
                                validJoinDates.add(updated_TS);
                            }
                        }
                        else{
                            Timestamp recentTS=updated_TS;
                            Calendar cal = Calendar.getInstance();
                            cal.setTime(recentTS);
                            cal.add(Calendar.DAY_OF_WEEK, 30);
                            Timestamp time_30=new Timestamp(cal.getTime().getTime());

                            if(recentTS.before(joinDT) && (time_30.after(joinDT) || joinDT.equals(time_30))){
                                updated_TS=time_30;
                                validJoinDates.add(recentTS);
                            }
                            else
                            {
                                validJoinDates.add(joinDT);
                            }
                        }

                    }
                    return validJoinDates;
                }
                , DataTypes.createArrayType(DataTypes.TimestampType));

    }
    public static StructType getEmpSchema() {

        StructField[] fields = {new StructField("emp_id", DataTypes.IntegerType, true, Metadata.empty())
                ,new StructField("emp_name", DataTypes.StringType, true, Metadata.empty()),
                new StructField("salary", DataTypes.DoubleType, true, Metadata.empty()),
              new StructField("join_date",DataTypes.TimestampType,true,Metadata.empty())};

        return new StructType(fields);
    }
}
