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
import java.io.*;

import ytj.*;
import jhx.*;
import jhx.bean.*;


/**
 * @author Kinglung Lee
 */
public class TestAll {

    private Runner runner = null;
    private String field;
    private boolean isOAP;
    private QueryResult queryWithIndexResult;
    private QueryResult queryWithoutIndexResult;

    private final String DEFAULT_FIELD = "age";
    private static final int RUN_TIMES = 20;

    public TestAll(boolean isOAP) {
        this.isOAP = isOAP;
    }

    public TestAll(String filename, boolean isOAP) {
        this.isOAP = isOAP;
        if (isOAP)
            runner = new OAPBTreeRunner(filename);
        else
            runner = new HBaseRunner(filename);
        this.field = DEFAULT_FIELD;
    }

    public TestAll(String filename, String field, boolean isOAP) {
        this.isOAP = isOAP;
        if (isOAP)
            runner = new OAPBTreeRunner(filename);
        else
            runner = new HBaseRunner(filename);
        this.field = field;
    }

    public void turnToOAP() {
        this.isOAP = true;
    }

    public void turnToHBase() {
        this.isOAP = false;
    }

    public boolean getFlag() {
        return isOAP;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public void resetRunner(String filename) {
        runner = null;
        System.gc();
        if (isOAP)
            runner = new OAPBTreeRunner(filename);
        else
            runner = new HBaseRunner(filename);
    }

    public long createIndex() {
        long timetmp = System.currentTimeMillis();
        runner.createIndex(field);
        long time = System.currentTimeMillis() - timetmp;
        return time;
    }

    public long runQuery(QueryCondition condition, boolean hasIndex) {
        long timetmp = System.currentTimeMillis();
        if (hasIndex)
            queryWithIndexResult = runner.query(condition);
        else
            queryWithoutIndexResult = runner.query(condition);
        long time = System.currentTimeMillis() - timetmp;
        return time;
    }

    @Test
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
        fs.close();
    }

    // jhx.Main
    public static void generateData() throws IOException, URISyntaxException, InterruptedException {
        final String saveDir = "out";
        final String suffix = ".json";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf, "root");
        for (int i = 1; i <= 1000; i *= 10) {
            File f = new File(saveDir + File.separator + i + "MB.json");
            if (!f.exists())
                f.createNewFile();
            String readPath = RandomUtil.generateJsonFile(saveDir, i);
            fs.copyFromLocalFile(new Path(readPath), new Path("/user/root/out/"));
            for (int j = 0; j < 10; j++) {
                QueryCondition qc = RandomUtil.getRandomCondition();
                // out/1MB-age > 20.json
                String resultPath = saveDir + File.separator + i + "MB-" + qc.toString() + suffix;
                RandomUtil.generateQueryResult(readPath, resultPath, qc);
                fs.copyFromLocalFile(new Path(resultPath), new Path("/user/root/out/"));
            }
            fs.close();
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

        TestAll t = new TestAll(true);
        for (int i = 1; i <= 100; i *= 10) {
            t.resetRunner("out/" + String.valueOf(i) + "MB.json");
            for (int j = 0; j < RUN_TIMES; j++) {
                t.resetHDFS();
                t.runner.before("");
                t.setField(qc.get(j).getField());
                timeWithoutIndex.add(t.runQuery(qc.get(j), false));
                t.runner.after("");
            }
            t.runner.before("");
            List<String> createdFields = new ArrayList<String>();
            for (int j = 0; j < RUN_TIMES; j++) {
                if (createdFields.contains(qc.get(j).getField()))
                    continue;
                createdFields.add(qc.get(j).getField());
                t.setField(qc.get(j).getField());
                timeCreateIndex += t.createIndex();
            }
            timeCreateIndex /= createdFields.size();
            for (int j = 0; j < RUN_TIMES; j++) {
                t.setField(qc.get(j).getField());
                //t.runner.before();
                timeWithIndex.add(t.runQuery(qc.get(j), true));
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
            s += ("Average time create index: " + String.valueOf(timeCreateIndex) + "ms");
            System.out.println(s);

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

            timeWithoutIndex.clear();
            timeWithIndex.clear();

            t.resetHDFS();
        }

        //System.out.println(time1);
        //System.out.println(time2);
    }
}