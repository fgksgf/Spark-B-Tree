package ytj;

import org.apache.spark.SparkConf;

public class DefaultConfLoader {
    public static final SparkConf getDefaultConf() {
        /**
         * If you use spark, it is optional for you to call this for default spark configuration
         */
        return new SparkConf().setJars(new String[] {"/home/bdms/homework/Spark-B-Tree-master/target/Bigdata-1.0-SNAPSHOT.jar"})
        .setMaster("local").setAppName("app")
        .set("spark.sql.extensions", "org.apache.spark.sql.OapExtensions")
        .set("spark.files", "/home/bdms/setup/oap/oap-0.6.1-with-spark-2.4.4.jar")
        .set("spark.executor.extraClassPath", "/home/bdms/setup/oap/oap-0.6.1-with-spark-2.4.4.jar")
        .set("spark.driver.extraClassPath", "/home/bdms/setup/oap/oap-0.6.1-with-spark-2.4.4.jar")
        .set("spark.sql.oap.parquet.data.cache.enable", "true")
        .set("spark.sql.oap.orc.data.cache.enable", "true").set("spark.sql.orc.copyBatchToSpark", "true")
        .set("spark.memory.offHeap.enabled", "true").set("spark.memory.offHeap.size", "4g")//.set("spark.memory.offHeap.size", "3g")
        .set("spark.sql.oap.parquet.data.cache.enable", "true")
        .set("spark.sql.oap.orc.data.cache.enable", "true").set("spark.sql.orc.copyBatchToSpark", "true");
    }
}