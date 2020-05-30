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

import breeze.util.Index;
import jhx.bean.QueryCondition;

import org.apache.spark.sql.OapExtensions;

import com.google.gson.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;


public class HBaseRunner extends Runner implements Indexable {

    private JavaSparkContext sc;
    private SQLContext sqlsc;

    private Dataset<Row> dataset;

    public HBaseRunner(String filename) {
        super(filename);
    }

    @Override
    public void before(String field) {
        this.sc = new JavaSparkContext(DefaultConfLoader.getDefaultConf());
        this.sqlsc = new SQLContext(sc);
        this.dataset = DefaultDataLoader.loadPropleDatasetWithView(this.sqlsc, this.fileName);
    }

    @Override
    public void createIndex(String field) {
        
    }

    @Override
    public void deleteIndex(String field) {
        
    }

    @Override
    public QueryResult query(QueryCondition condition) {
        return null;
    }

    public static void main(String[] args) {
        // OAPBTreeRunner r = new OAPBTreeRunner("out/1MB.json");

        // r.before("age");
        // r.createIndex("age");

        // QueryResult res = r.query(new QueryCondition("age < 30"));
        // for(Long idx: res) {
        //     System.out.println(idx);
        // }
        // r.after("age");
        Runner r = new HBaseRunner("out/1MB.json");
        System.out.println(r instanceof Indexable);
    }

    @Override
    public void after(String field) {
        // TODO Auto-generated method stub
        this.sc.close();
    }

    
}
