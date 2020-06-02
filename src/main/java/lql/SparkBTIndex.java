package lql;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.google.gson.Gson;
import jhx.bean.QueryCondition;
import org.apache.avro.io.JsonEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.util.DatasetUtils;
import org.apache.spark.sql.*;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.mapdb.*;
import javafx.util.Pair;
import scala.collection.Seq;
import ytj.Indexable;
import ytj.QueryResult;
import ytj.Runner;

import static org.apache.spark.sql.functions.*;

public class SparkBTIndex extends Runner implements Indexable {

    private JavaSparkContext sc;

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
        try {
            Pair<ArrayList<Integer>, ArrayList<Integer>> JSONOffset = GetJSONOffset(fileName);
            SQLContext ssc = new SQLContext(sc);
            StructType schema = new StructType().add("age", DataTypes.IntegerType)
                    .add("salary", DataTypes.IntegerType)
                    .add("sex", DataTypes.StringType)
                    .add("name", DataTypes.StringType)
                    .add("features", DataTypes.StringType);
            df = ssc.jsonFile(fileName, schema);
            df.col(field);
            Dataset<Row> offset = ssc.createDataFrame(JSONOffset.getKey(), Integer.class);
            Dataset<Row> length = ssc.createDataFrame(JSONOffset.getValue(), Integer.class);
            Dataset<Row> address = offset.join(length);
            Dataset<Row> pair = address.withColumn(field, df.col(field));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createIndex(String field) {
        DB db = DBMaker.fileDB(new File("BTIndex_" + field))
                .closeOnJvmShutdown()
                .make();
        BTreeMap<Integer, ArrayList<Integer[]>> Btree = db.get("btmap");
        df.sort(asc(field))
            .foreachPartition((ForeachPartitionFunction<Row>) it -> {
                DB db1 = DBMaker.memoryDB().make();
                BTreeMap<Integer, ArrayList<Integer[]>> map = (BTreeMap<Integer, ArrayList<Integer[]>>) db1.treeMap("map")
                                .keySerializer(Serializer.INTEGER)
                                .createOrOpen();
                while (it.hasNext()){
                    Row r = it.next();
                    Integer key = r.getAs(field);
                    map.putIfAbsent(key, new ArrayList<>());
                    map.get(key).add(new Integer[]{r.getAs("offset"), r.getAs("length")});
                }
                Btree.putAll(map);
            });
        db.commit();
    }

    @Override
    public void deleteIndex(String field) {
        return;
    }

    @Override
    public void after(String field) {
        sc.close();
    }

    @Override
    public QueryResult query(QueryCondition condition) {
        // load BTree
        DB db = DBMaker.fileDB(new File("BTIndex_" + condition.getField())).make();
        BTreeMap<Integer, Integer[]> Btree = db.get("btmap");
        Iterator<Integer[]> V = null;

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
            
            boolean arrowDirection = false;
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
        JavaRDD<ArrayList<Integer[]>> RDD = sc.parallelize((ArrayList<ArrayList<Integer[]>>) V);
        JavaRDD<Long> res_id = RDD.flatMap((FlatMapFunction<ArrayList<Integer[]>, Integer[]>) integers -> integers.iterator())
                .sortBy((Function<Integer[], Long>) integers -> Long.valueOf(integers[0]), true, 1)
                .mapPartitions((FlatMapFunction<Iterator<Integer[]>, Row>) it -> {
                    FileSystem fs = FileSystem.get(URI.create(fileName), new Configuration());
                    FSDataInputStream in_stream = fs.open(new Path(fileName));
                    ArrayList<String> res = new ArrayList<>();
                    while (it.hasNext()){
                        Integer[] n = it.next();
                        int offset = n[0], length = n[1];

                        // load content
                        byte[] buffer = new byte[length];
                        in_stream.read(offset, buffer, 0, length);
                        String json = new String(buffer);
                        res.add(json);
                    }
                    SQLContext ssc = new SQLContext(sc);
                    Dataset<Row> rows = ssc.read().json(sc.parallelize(res));
                    return rows.collectAsList().iterator();
            }).map((Function<Row, Long>) row -> row.getAs("id"));
        List<Long> res_id_list = res_id.collect();
        QueryResult ret = new QueryResult(res_id_list);
        return ret;
    }

    public static Pair<ArrayList<Integer>, ArrayList<Integer>> GetJSONOffset(String FilePath) throws IOException {
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
        return JSONOffset;
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