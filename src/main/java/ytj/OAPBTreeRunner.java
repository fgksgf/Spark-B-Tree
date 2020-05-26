package ytj;

import org.apache.spark.SparkConf;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.codehaus.janino.Java;

import jhx.bean.QueryCondition;

import org.apache.spark.sql.OapExtensions;

import com.google.gson.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;


public class OAPBTreeRunner extends Runner {

    private JavaSparkContext sc;
    private SQLContext sqlsc;

    private Dataset<Row> dataset;

    public OAPBTreeRunner(String filename) {
        super(filename);
        this.sc = new JavaSparkContext(DefaultConfLoader.getDefaultConf());
        this.sqlsc = new SQLContext(sc);
        this.dataset = DefaultDataLoader.loadPropleDatasetWithView(this.sqlsc, filename);
    }

    @Override
    public void afterCreateIndex(String field) {
        
    }

    @Override
    public void afterDeleteIndex(String field) {

    }

    @Override
    public void beforeCreateIndex(String field) {
        sqlsc.sql("create table oap_test(age int, salary int, sex string, name string, features string) using parquet OPTIONS (path 'hdfs://localhost:9000/user/oap/')");
        sqlsc.sql("insert overwrite table oap_test select * from people");
    }

    @Override
    public void beforeDeleteIndex(String field) {

    }

    @Override
    public void createIndex(String field) {
        String sql = String.format("create oindex idx_%s on oap_test(%s) using btree", field, field);
        sqlsc.sql(sql);
    }

    @Override
    public void deleteIndex(String field) {
        sqlsc.sql("drop oindex idx_" + field);
    }

    @Override
    public QueryResult queryWithIndex(QueryCondition condition) {
        String sql = "select * from oap_test where " + condition.toString();
        Dataset<Row> res = sqlsc.sql(sql);
        Dataset<Long> res_id = res.map((Row r) -> {
            return r.getAs("id");
        }, Encoders.LONG());
        List<Long> res_id_list = res_id.collectAsList();
        QueryResult ret = new QueryResult(res_id_list);
        return ret;
    }

    public static void main(String[] args) {
        Runner r = new OAPBTreeRunner("out/1MB.json");

        r.beforeCreateIndex("age");
        r.createIndex("age");
        r.afterCreateIndex("age");
        QueryResult res = r.queryWithIndex(new QueryCondition("age < 30"));
        for(Long idx: res) {
            System.out.println(idx);
        }
    }

    
}
