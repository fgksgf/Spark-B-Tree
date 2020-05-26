package ytj;

import org.apache.spark.SparkConf;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.codehaus.janino.Java;
import org.apache.spark.sql.OapExtensions;

public class Main {

    public static void main(String[] args) {
        
        SparkConf conf = new SparkConf().setMaster("local").setAppName("app").
        set("spark.sql.extensions", "org.apache.spark.sql.OapExtensions").
        set("spark.files", "/home/bdms/setup/oap/oap-0.6.1-with-spark-2.4.4.jar").
        set("spark.executor.extraClassPath", "/home/bdms/setup/oap/oap-0.6.1-with-spark-2.4.4.jar").
        set("spark.driver.extraClassPath", "/home/bdms/setup/oap/oap-0.6.1-with-spark-2.4.4.jar").
        set("spark.sql.oap.parquet.data.cache.enable", "true").
        set("spark.sql.oap.orc.data.cache.enable", "true").
        set("spark.sql.orc.copyBatchToSpark", "true").
        set("spark.memory.offHeap.enabled", "true").
        set("spark.memory.offHeap.size", "3g").
        set("spark.sql.oap.parquet.data.cache.enable", "true").
        set("spark.sql.oap.orc.data.cache.enable", "true").
        set("spark.sql.orc.copyBatchToSpark", "true");

        String v = conf.get("spark.files", "xxxx");
        System.out.println(v);
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext ssc = new SQLContext(sc);
        ssc.sql("CREATE TABLE oap_test (a INT, b STRING) USING parquet OPTIONS (path 'hdfs://localhost:9000/user/oap/')");
        JavaPairRDD<Integer, String> dat = 
        ssc.sql("create oindex index1 on oap_test (a)");
        ssc.sql("show oindex from oap_test").show();

    }
}
