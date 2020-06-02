package lql;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.functions.*;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions.*;

import org.mapdb.*;
import javafx.util.Pair;

public class SparkBTIndex extends Runner implements Indexable {

    private JavaSparkContext sc;
    private SQLContext ssc;

    private Dataset<Row> df;

    public SparkBTIndex(String fileName) {
        super(fileName);
    }

    @Override
    public void before(String field) {
        // Initial Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkBTIndex");
        sc = new JavaSparkContext(conf);

        // Load JSON
        Pair<ArrayList<Integer>, ArrayList<Integer>> JSONOffset = GetJSONOffset(fileName); 
        ssc = new SQLContext(sc);
        StructType schema = new StructType().add("age", DataTypes.IntegerType)
                            .add("salary", DataTypes.IntegerType)
                            .add("sex", DataTypes.StringType)
                            .add("name", DataTypes.StringType)
                            .add("features", DataTypes.StringType);
        df = ssc.jsonFile(fileName, schema);

        df.withColumn("offset", JSONOffset.getKey());
        df.withColumn("length", JSONOffset.getValue());
    }

    @Override
    public void createIndex(String field) {
        BTreeMap<Integer, Integer[]> map = df
            .sort(asc(field))
            .foreachPartition(new JavaForeachPartitionFunc() {
                @Override
                public BTreeMap<Integer, Integer[]> call(Iterator<Row> it) {
                    BTreeMap<Integer, Array<Integer[]>> map = db.treeMap("map")
                                    .keySerializer(Serializer.Integer)
                                    .createOrOpen();
                    while (it.hasNext()){
                        Row r = it.next();
                        Integer key = r.getAs(field);
                        map.putIfAbsent(key, new Array<>());
                        map.get(key).add(new Integer[]{r.getAs("offset"), r.getAs("length")});
                    }
                    return map;
                }
            }).reduce(new Function2<BTreeMap<Integer, Integer[]>, BTreeMap<Integer, Integer[]>, BTreeMap<Integer, Integer[]>>() {
                @Override
                public BTreeMap<Integer, Integer[]> call(BTreeMap<Integer, Integer[]> map1, BTreeMap<Integer, Integer[]> map2) throws Exception {
                    map1.putAll(map2);
                    return map1;
                }
            });
        DB db = DBMaker.newFileDB(new File("BTIndex_" + field))
            .closeOnJvmShutdown()
            .make();
        ConcurrentNavigableMap<Integer, Integer[]> Btree = db.getTreeMap("btmap");
        Btree.putAll(map);
        db.commit();
    }

    @Override
    public void deleteIndex(String field) {
        return;
    }

    @Override
    public QueryResult query(QueryCondition condition) {
        // load BTree
        DB db = DBMaker.newFileDB(new File("BTIndex_" + field)).make();
        BTreeMap<Integer, Integer[]> Btree = db.getTreeMap("btmap");
        Iterator<Integer[]> V;

        // range fliter
        if (condition.isTypeOne()) {
            String operator = condition.getOperator();
            int value = condition.getValue();

            switch (operator) {
                case ">":
                    V = Btree.valueIterator(value + 1, false, 0, true);
                    break;
                case ">=":
                    V = Btree.valueIterator(value, false, 0, true);
                    break;
                case "<":
                    V = Btree.valueIterator(0, true, value - 1, false);
                    break;
                case "<=":
                    V = Btree.valueIterator(0, true, value, false);
                    break;
            }
        } else {
            String loperator = condition.getLeftOperator();
            String roperator = condition.getRightOperator();
            int lvalue = condition.getLeftValue();
            int rvalue = condition.getRightValue();
            
            Bool arrowDirection = false;
            switch (loperator) {
                case ">":
                    lvalue -= 1;
                    arrowDirection = true;
                    break;
                case "<":
                    lvalue += 1;
                    break;
            }
            switch (roperator) {
                case ">":
                    rvalue += 1;
                    break;
                case "<":
                    rvalue -= 1;
                    break;
            }
            if (arrowDirection) {
                V = Btree.valueIterator(rvalue, false, lvalue, false);
            } else {
                V = Btree.valueIterator(lvalue, false, rvalue, false);
            }
        }

        // Load data using spark
        JavaRDD<Integer[]> RDD = sc.parallelize(V);
        RDD.sort(asc(field))
            .foreachPartition(new JavaForeachPartitionFunc() {
                @Override
                public BTreeMap<Integer, Integer[]> call(Iterator<Integer[]> it) {
                    FileSystem fs = FileSystem.get(URI.create(FilePath), new Configuration());
                    FSDataInputStream in_stream = fs.open(new Path(FilePath));
                    while (it.hasNext()){
                        Integer[] n = it.next();
                        int offset = n[0], length = n[1];

                        // load content
                        byte[] buffer = new byte[length];
                        in_stream.read(offset, buffer, 0, length);
                        String json = new String(buffer);

                        // TODO parse json and generate new Row
                        Row r = new Row();
                    }
                    return map;
                }
            })
            .map((Row r) -> {
                return r.getAs("id");
            }, Encoders.LONG());
        List<Long> res_id_list = res_id.collectAsList();
        QueryResult ret = new QueryResult(res_id_list);
        return ret;
    }

    public static Pair<Integer[], Integer[]> GetJSONOffset(String FilePath) {
        // Prepare for HDFS reading
        FileSystem fs = FileSystem.get(URI.create(FilePath), new Configuration());
        FSDataInputStream in_stream = fs.open(new Path(FilePath));
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));

        // Load file
        Pair<ArrayList<Integer>, ArrayList<Integer>> JSONOffset = new Pair<>(new ArrayList<>(), new ArrayList<>());
        
        String line;
        int leftp = 0, count = 0;
        int offset = 0, length = 0;
        while ((line = in.readLine()) != null) {
            for(int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '{') {
                    if (count == 0) {
                        leftp = offset + i;
                    }
                    count++;
                } else if (c == '}') {
                    count--;
                    // A record
                    if (count == 0) {
                        length = offset + i - leftp;
                        JSONOffset.getKey().add(leftp);
                        JSONOffset.getValue().add(length);
                    }
                }
            }
            offset += line.length();
        }
        in.close();
        fs.close();
    }

    @Override
    public void after(String field) {
        sc.close();
    }

    public static void main(String[] args) {
        SparkBTIndex r = new SparkBTIndex("out/1MB.json");

        r.before("age");
        r.createIndex("age");

        QueryResult res = r.query(new QueryCondition("age < 30"));
        for(Long idx: res) {
            System.out.println(idx);
        }
        r.after("age");
    }
}