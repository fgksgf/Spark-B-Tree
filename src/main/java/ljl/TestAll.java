package ljl;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;
import java.io.*;

import ytj.*;
import jhx.*;
import jhx.bean.*;

import org.apache.spark.sql.*;

/**
 * @author Kinglung Lee
 */
public class TestAll {

    private OAPBTreeRunner runner;
    private String field;
    private QueryResult queryWithIndexResult;
    private QueryResult queryWithoutIndexResult;

    private final String DEFAULT_FIELD = "age";
    private static final int RUN_TIMES = 5;

    public TestAll() {
    }

    public TestAll(String filename) {
        runner = new OAPBTreeRunner(filename);
        this.field = DEFAULT_FIELD;
    }

    public TestAll(String filename, String field) {
        runner = new OAPBTreeRunner(filename);
        this.field = field;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void resetRunner(String filename) {
        this.runner = new OAPBTreeRunner(filename);
    }

    public long createIndex() {
        long timetmp = System.currentTimeMillis();
        runner.createIndex(field);
        long time = System.currentTimeMillis() - timetmp;
        return time;
    }

    public long runQuery(QueryCondition condition) {
        long timetmp = System.currentTimeMillis();
        runner.query(condition);
        long time = System.currentTimeMillis() - timetmp;
        return time;
    }

    //@Test
    public void testResult(QueryCondition condition) {
        List<Long> l1 = queryWithoutIndexResult.getAll();
        List<Long> l2 = queryWithIndexResult.getAll();
        assertTrue((l1.size() == l2.size()) && l1.containsAll(l2));
    }

    public void resetHDFS() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "root");
        fs.delete(new Path("/user/oap"), true);
        fs.mkdirs(new Path("/user/oap"));
    }

    public void writeHDFS(String s) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "root");
        fs.delete(new Path("/user/oap"), true);
        fs.mkdirs(new Path("/user/oap"));
    }

    // jhx.Main
    public static void generateData() throws IOException {
        final String saveDir = "out";
        final String suffix = ".json";
        for (int i = 1; i <= 1000; i *= 10) {
            File f = new File(saveDir + File.separator + i + "MB.json");
            if (!f.exists())
                f.createNewFile();
            String readPath = RandomUtil.generateJsonFile(saveDir, i);
            for (int j = 0; j < 10; j++) {
                QueryCondition qc = RandomUtil.getRandomCondition();
                // out/1MB-age > 20.json
                String resultPath = saveDir + File.separator + i + "MB-" + qc.toString() + suffix;
                RandomUtil.generateQueryResult(readPath, resultPath, qc);
            }
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {

        //generateData();

        List<Long> timeWithoutIndex = new ArrayList<Long>(5);
        List<Long> timeWithIndex = new ArrayList<Long>(5);
        long timeCreateIndex = 0L;
        List<QueryCondition> qc = new ArrayList<QueryCondition>(5);
        for (int i = 0; i < RUN_TIMES; i++)
            qc.add(i, RandomUtil.getRandomCondition());

        TestAll t = new TestAll();
        for (int i = 1; i <= 100; i *= 10) {
            t.resetRunner("out/" + String.valueOf(i) + "MB.json");
            t.runner.before("");
            for (int j = 0; j < RUN_TIMES; j++) {
                t.setField(qc.get(j).getField());
                timeWithoutIndex.add(t.runQuery(qc.get(j)));
            }
            timeCreateIndex = t.createIndex();
            for (int j = 0; j < RUN_TIMES; j++) {
                t.setField(qc.get(j).getField());
                //t.runner.before();
                timeWithIndex.add(t.runQuery(qc.get(j)));
            }
            t.runner.after("");

            String s = "";
            for (int j = 0; j < RUN_TIMES; j++) {
                //System.out.println(timeWithoutIndex.get(j));
                s += (qc.get(j).toString() + ": Time without index: " + String.valueOf(timeWithoutIndex.get(j)) + "ms\n");
                //fileWriter.flush();
            }
            for (int j = 0; j < RUN_TIMES; j++) {
                s += (qc.get(j).toString() + ": Time with index: " + String.valueOf(timeWithIndex.get(j)) + "ms\n");
                //fileWriter.flush();
            }
            s += ("Time create index: " + String.valueOf(timeCreateIndex) + "ms");
            System.out.println(s);

            Configuration conf = new Configuration();
            try {
                File f = new File("/home/bdms/homework/ResultOf" + String.valueOf(i) + "MB.txt");
                if (!f.exists())
                    f.createNewFile();
                OutputStream output = new FileOutputStream(new File(f.getPath()));
                output.write(s.getBytes("ASCII"));
                output.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            t.resetHDFS();
        }
        //System.out.println(time1);
        //System.out.println(time2);
    }
}