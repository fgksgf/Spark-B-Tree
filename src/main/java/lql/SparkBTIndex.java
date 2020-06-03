package lql;

import java.io.*;
import java.lang.reflect.Array;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;

import com.fasterxml.jackson.databind.ser.std.ArraySerializerBase;
import com.twitter.chill.java.ArraysAsListSerializer;
import jhx.bean.QueryCondition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;

import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.util.ClosureCleaner$;
import org.mapdb.*;
import parquet.org.codehaus.jackson.map.ser.ArraySerializers;
import parquet.org.codehaus.jackson.map.ser.std.StdArraySerializers;
import ytj.Indexable;
import ytj.QueryResult;
import ytj.Runner;

import static org.apache.spark.sql.functions.*;

public class SparkBTIndex extends Runner implements Indexable {

    private JavaSparkContext sc;

    private Dataset<Row> ds;

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
            ArrayList<Address> JSONAddress = GetJSONAddress(fileName);
            SQLContext ssc = new SQLContext(sc);
            StructType schema = new StructType().add("age", DataTypes.IntegerType)
                    .add("salary", DataTypes.IntegerType)
                    .add("sex", DataTypes.StringType)
                    .add("name", DataTypes.StringType)
                    .add("features", DataTypes.StringType);
            Dataset<Row> json = ssc.jsonFile(fileName, schema);
            json = json.withColumn("__id", row_number().over(Window.orderBy(lit(1))));
            Dataset<Row> address = ssc.createDataFrame(JSONAddress, Address.class);
            address = address.withColumn("__id", row_number().over(Window.orderBy(lit(1))));
            ds = json.join(address, "__id");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createIndex(String field) {
        DB db = DBMaker.fileDB(new File("BTIndex_" + field))
                .closeOnJvmShutdown()
                .make();
        BTreeMap<Integer, int[]> Btree = (BTreeMap<Integer, int[]>) db.treeMap("btmap")
                .keySerializer(Serializer.INTEGER)
                .valueSerializer(Serializer.INT_ARRAY)
                .createOrOpen();
        List<BTreeMap<Integer, int[]>> result = ds.sort(asc(field))
                .toJavaRDD()
                .mapPartitions((FlatMapFunction<Iterator<Row>, BTreeMap<Integer, int[]>>) it -> {
                    DB db1 = DBMaker.memoryDB().make();
                    BTreeMap<Integer, int[]> map = (BTreeMap<Integer, int[]>) db1.treeMap("map")
                            .keySerializer(Serializer.INTEGER)
                            .valueSerializer(Serializer.INT_ARRAY)
                            .createOrOpen();

                    while (it.hasNext()){
                        Row r = it.next();
                        Integer key = r.getAs(field);
                        map.putIfAbsent(key, new int[]{});
                        int a[] = map.get(key);
                        int b[] = Arrays.copyOf(a, a.length + 2);
                        b[a.length] = r.getAs("offset");
                        b[a.length + 1] = r.getAs("length");
                        map.put(key, b);
                    }
                    db1.commit();
                    ArrayList<BTreeMap<Integer, int[]>> res = new ArrayList<>();
                    res.add(map);
                    return res.iterator();
                }).collect();
        for (java.util.concurrent.ConcurrentMap t : result) {
            Btree.putAll(t);
        }
        db.commit();
        db.close();
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
        BTreeMap<Integer, int[]> Btree = (BTreeMap<Integer, int[]>) db.treeMap("btmap").createOrOpen();
        Iterator<int[]> V = null;

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
        // Add each element of iterator to the List
        ArrayList<int[]> list = new ArrayList<>();
        V.forEachRemaining(list::add);
        // Load data using spark
        JavaRDD<int[]> RDD = sc.parallelize(list);
        JavaRDD<Long> res_id = RDD.flatMap((FlatMapFunction<int[], Integer[]>) integers -> {
                    ArrayList<Integer[]> address = new ArrayList<>();
                    for (int i = 0;i < integers.length;i+=2)
                        address.add(new Integer[]{integers[i], integers[i+1]});
                    return address.iterator();
                })
                .sortBy((Function<Integer[], Long>) integers -> Long.valueOf(integers[0]), true, 1)
                .mapPartitions((FlatMapFunction<Iterator<Integer[]>, Row>) it -> {
                    FileSystem fs = FileSystem.get(new Configuration());
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
                    Dataset<Row> rows = new SQLContext(sc).read().json(sc.parallelize(res));
                    fs.close();
                    in_stream.close();
                    return rows.collectAsList().iterator();
                }).map((Function<Row, Long>) row -> row.getAs("id"));
        List<Long> res_id_list = res_id.collect();
        QueryResult ret = new QueryResult(res_id_list);
        return ret;
    }

    public static ArrayList<Address> GetJSONAddress(String FilePath) throws IOException {
        // Prepare for HDFS reading
        FileSystem fs = FileSystem.get(URI.create(FilePath), new Configuration());
        FSDataInputStream in_stream = fs.open(new Path(FilePath));
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));

        // Load file
        ArrayList<Address> JSONAddress = new ArrayList<>();

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
                        Address a = new Address();
                        a.setLength(length);
                        a.setOffset(leftp);
                        JSONAddress.add(a);
                    }
                }
            }
            offset += line.length();
        }
        in.close();
        in_stream.close();
        fs.close();
        return JSONAddress;
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