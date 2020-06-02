package lql;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;

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


public class BuildBTIndex {

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

    public static void main(String[] args) {
        String JSONFILE = args[1];
        String[] AREAS = Arrays.copyOfRange(args, 2, args.length);
        
        // Initial Spark
        SparkConf conf = new SparkConf().setMaster("local").setAppName("BuildBTIndex");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load JSON
        Pair<ArrayList<Integer>, ArrayList<Integer>> JSONOffset = GetJSONOffset(JSONFILE); 
        SQLContext ssc = new SQLContext(sc);
        StructType schema = new StructType().add("age", DataTypes.IntegerType)
                            .add("salary", DataTypes.IntegerType)
                            .add("sex", DataTypes.StringType)
                            .add("name", DataTypes.StringType)
                            .add("features", DataTypes.StringType);
        Dataset<Row> df = ssc.jsonFile(JSONFILE, schema);
       // DataFrame df = ssc.read().json("JSONFILE");

        df.withColumn("offset", JSONOffset.getKey());
        df.withColumn("length", JSONOffset.getValue());

        for (int i = 0;i < AREAS.length;i++) {
            BTreeMap<Integer, Pair<Integer, Integer>> map = df
                .sort(asc(AREAS[i]))
                .foreachPartition(new JavaForeachPartitionFunc() {
                    @Override
                    public BTreeMap<Integer, Pair<Integer, Integer>> call(Iterator<Row> it) {
                        BTreeMap<Integer, Array<Pair<Integer, Integer>>> map = db.treeMap("map")
                                        .keySerializer(Serializer.Integer)
                                        .createOrOpen();
                        while (it.hasNext()){
                            Row r = it.next();
                            Integer key = r.getAs(AREAS[i]);
                            map.putIfAbsent(key, new Array<>());
                            map.get(key).add(new Pair<Integer, Integer>(r.getAs("offset"), r.getAs("length")));
                        }
                        return map;
                    }
                }).reduce(new Function2<BTreeMap<Integer, Pair<Integer, Integer>>, BTreeMap<Integer, Pair<Integer, Integer>>, BTreeMap<Integer, Pair<Integer, Integer>>>() {
                    @Override
                    public BTreeMap<Integer, Pair<Integer, Integer>> call(BTreeMap<Integer, Pair<Integer, Integer>> map1, BTreeMap<Integer, Pair<Integer, Integer>> map2) throws Exception {
                        map1.putAll(map2);
                        return map1;
                    }
                });
            DB db = DBMaker.newFileDB(new File("BTIndex_" + AREAS[i]))
                .closeOnJvmShutdown()
                .make();
            ConcurrentNavigableMap<Integer, Pair<Integer, Integer>> Btree = db.getTreeMap("btmap");
            Btree.putAll(map);
            db.commit();
        }
    }
}