import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import sparktest.Dept;
import sparktest.Emp;
import sparktest.Main;
import org.junit.*;

import java.util.*;

public class empTest {

    static  SparkSession spark;
    static Dataset<Emp> empDataset;
    static Dataset<Dept> deptDataset;

    @BeforeClass
    public static void setUp(){

        spark=SparkSession
                .builder()
               .master("local[*]")
           //  .config("hive.metastore.uris","thrift://hadoop3.dev.clairvoyant.local:9083")
                // .config("hive.metastore.warehouse.dir","/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        spark.udf().register("empIdUDF",
                (Integer empID)-> "EMP-TEST-"+Integer.toString(empID)
                , DataTypes.StringType);

        Dataset<Row>  empData = spark.read()
                .option("header", "true")
                .schema(Main.getEmpSchema())
                .csv("src/main/resources/emp.csv");

        Dataset<Row> deptData = spark.read()
                .option("header", "true")
                .schema(Main.getDeptSchema())
                .csv("src/main/resources/dept.csv");

        empDataset=empData
                .na().fill(0,new String[]{"dept_id"}).as(Encoders.bean(Emp.class));

        deptDataset = deptData.as(Encoders.bean(Dept.class));

    }


   @Test
    public void testMaxEmpSalByDept(){

       Map<Integer,Double> expectedMap=new HashMap<>();
       expectedMap.put(10,90000.00);
       expectedMap.put(11,13000.00);
       expectedMap.put(12,60000.00);
       expectedMap.put(0,12000.00);

       Map<Integer,Double> actualMap=new HashMap<>();

       List<Row> actualResult=Main.getMaxSalEmpByDept(empDataset,deptDataset)
               .select("dept_id","salary").collectAsList();
       actualResult.forEach( a->actualMap.put(a.getInt(0),a.getDouble(1)));

       Assert.assertTrue(compareMaps(actualMap,expectedMap));

    }

    @Test
    public void testTotalSalByDept(){

        Map<Integer,Double> expectedMap=new HashMap<>();
        expectedMap.put(10,130000.00);
        expectedMap.put(11,32000.00);
        expectedMap.put(12,85000.00);
        expectedMap.put(0,22000.00);

        Map<Integer,Double> actualMap=new HashMap<>();

        List<Row> actualResult=Main.getTotalSalByDept(empDataset,deptDataset)
                .select("dept_id","total_salary").collectAsList();
        actualResult.forEach( a->actualMap.put(a.getInt(0),a.getDouble(1)));

        Assert.assertTrue(compareMaps(actualMap,expectedMap));
    }
    @Test
    public  void  testCollectedSalByDept(){
        Map<Integer,List<Double>> expectedMap=new HashMap<>();

        Double[] empSal_10={15000.0,25000.0,90000.0};
        Double[] empSal_11={9000.0,10000.0,13000.0};
        Double[] empSal_12={10000.0,15000.0,60000.0};
        Double[] empSal_0={10000.0,12000.0};



        expectedMap.put(10, Arrays.asList(empSal_10));
        expectedMap.put(11,Arrays.asList(empSal_11));
        expectedMap.put(12,Arrays.asList(empSal_12));
        expectedMap.put(0,Arrays.asList(empSal_0));

        Map<Integer,List<Double>> actualMap=new HashMap<>();


       List<Row> actualResult= Main.collectEmpSalariesByDept(empDataset,deptDataset)
               .select("dept_id","collect_salary").collectAsList();

        actualResult
                .forEach(a->
                {
                    List<Double> l1=new ArrayList<>(a.getList(1));
                    Collections.sort(l1);
                    actualMap.put(a.getInt(0), l1);
                }
                );

        Assert.assertTrue(compareLists(actualMap,expectedMap));



    }

    public boolean compareMaps(Map<Integer,Double> actualResult,Map<Integer,Double> expectedResult){
        try{
            for (Integer k : expectedResult.keySet())
            {
                if (!actualResult.get(k).equals(expectedResult.get(k))) {
                    return false;
                }
            }
            for (Integer y : actualResult.keySet())
            {
                if (!actualResult.get(y).equals(expectedResult.get(y))) {
                    return false;
                }
            }
        } catch (NullPointerException np) {
            return false;
        }
        return true;


    }
    public boolean compareLists(Map<Integer,List<Double>> actualResult,Map<Integer,List<Double>> expectedResult){
        try{
            for (Integer k : expectedResult.keySet())
            {
                List<Double> actualList=actualResult.get(k);

                if (!actualList.equals(expectedResult.get(k))) {
                    return false;
                }
            }
            for (Integer y : actualResult.keySet())
            {
                List<Double> actualList=actualResult.get(y);

                if (!actualList.equals(expectedResult.get(y))) {
                    return false;
                }
            }
        } catch (NullPointerException np) {
            return false;
        }
        return true;

    }
}
