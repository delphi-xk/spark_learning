package com.hyzs.java.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by Administrator on 2017/9/1.
 */
public class SparkJavaTest {

    public static void main(String[] args){
        String appName = "testSpark";

        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc.sc());
        //SQLContext sqlContext = new SQLContext(sc.sc());

        String endDate = "20171030";
        int slotNum = 10;
        String startDate = "20171011";
        long slotLength = 86400*2;
        sqlContext.sql("use hyzs");
        sqlContext.sql("drop table his_test_swp");
        sqlContext.sql("drop table his_result");
        String hisSql = String.format("select his_data2.id, his_data2.price, " +
                " floor( (UNIX_TIMESTAMP('%2$s','yyyyMMdd') - UNIX_TIMESTAMP(create_date,'yyyyMMdd'))/ %3$d ) as stamp  " +
                " from his_data2 " +
                " where create_date >= '%1$s' and create_date <= '%2$s' ", startDate, endDate, slotLength);
        DataFrame swp = sqlContext.sql(hisSql);
        DataFrame tmpData = swp.groupBy("id", "stamp")
                .agg(functions.sum("price").as("sum_price"),
                        functions.avg("price").as("avg_price"),
                        functions.count("id").as("count_id"));
        DataFrame ids = swp.select("id").distinct().orderBy("id");
        for(int index=0;index<slotNum;index++){
            DataFrame s = tmpData.filter(" stamp = "+index);
            ids = ids.join(s, ids.col("id").equalTo(s.col("id")), "left_outer")
            .select(ids.col("*"),
                    s.col("count_id").as("count_id_"+index),
                    s.col("sum_price").as("sum_price_"+index),
                    s.col("avg_price").as("avg_price_"+index));

        }
        ids.saveAsTable("hyzs.his_result");

        //dataFrame.saveAsTable("his_test_swp");



        //dataFrame.javaRDD().foreach(x-> System.out.println(x.getString(0)+","+x.getString(1)) );
        /*
        JavaRDD<String> res = dataFrame.javaRDD().map((Row row)-> {
            return row.getString(0)+"|"+row.getFloat(1)+"|"+row.getString(2);
        });
        */
        /*
        if (args[0] != null && !args.equals("")){
            res.saveAsTextFile("hdfs://master:9000/hyzs/"+args[0]);
        }*/

        /*
        List<String> res = dataFrame.javaRDD().map((row)-> {
            return row.getString(1)+"|";
        }).collect();
        */



        sc.stop();
    }
}
