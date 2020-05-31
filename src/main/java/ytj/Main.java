package ytj;

import org.apache.spark.SparkConf;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.janino.Java;
import org.apache.spark.sql.OapExtensions;
import com.google.gson.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import org.apache.spark.storage.*;
import org.apache.spark.sql.execution.datasources.oap.index.IndexUtils;

public class Main {

    public static SparkConf getConf() {
        return new SparkConf().setMaster("local").setAppName("app").
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
    }

    public static void main(String[] args) {
        SparkConf conf = getConf();
        
        JavaSparkContext sc = new JavaSparkContext(conf);
        // sc.setCheckpointDir("hdfs://localhost:9000/ckpt");
        SQLContext ssc = new SQLContext(sc);
        
        StructType schema = new StructType().add("age", DataTypes.IntegerType)
                            .add("salary", DataTypes.IntegerType)
                            .add("sex", DataTypes.StringType)
                            .add("name", DataTypes.StringType)
                            .add("features", DataTypes.StringType);
        Dataset<Row> dataset = ssc.jsonFile("out/1MB.json", schema);
        dataset.createOrReplaceTempView("people");

        ssc.sql("create table oap_test(age int, salary int, sex string, name string, features string) using parquet OPTIONS (path 'hdfs://localhost:9000/user/oap/')");
        ssc.sql("insert overwrite table oap_test select * from people");
        ssc.sql("create oindex idx on oap_test(age) using btree");
        Dataset<Row> selected = ssc.sql("select * from oap_test where age > 48");
        selected.show();

        // ssc.sql("create table oap_test (age int) using parquet OPTIONS (path 'hdfs://localhost:9000/user/oap/')");
        // StructType schema = new StructType().add("age", DataTypes.IntegerType);
        // Dataset<Row> dataset = ssc.jsonFile("out/test.json", schema);
        // dataset.createOrReplaceTempView("people");

        // ssc.sql("insert overwrite table oap_test select * from people");

        // ssc.sql("create oindex idx on oap_test(age)");
        // Dataset<Row> selected = ssc.sql("select * from oap_test where age = 3");
        // System.out.println("================================================");
        // selected.show();
        // System.out.println("================================================");
    }
}
