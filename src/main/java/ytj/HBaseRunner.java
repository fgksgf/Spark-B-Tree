package ytj;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.it.unimi.dsi.fastutil.ints.IntComparator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.client.Result;

import java.util.ArrayList;
import java.util.List;

import jhx.bean.QueryCondition;
import scala.Tuple2;

public class HBaseRunner extends Runner implements Indexable {

    private JavaSparkContext sc;
    private SQLContext sqlsc;

    private Dataset<Row> dataset;

    private Configuration hbaseConf;
    private JobConf hbaseJobConf;

    private String tableName = "people";

    public HBaseRunner(String fileName) {
        super(fileName);

        System.out.println("2");
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "127.0.0.1");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");

        hbaseJobConf = new JobConf(hbaseConf);
        hbaseJobConf.setOutputFormat(TableOutputFormat.class);
        hbaseJobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName);

    }

    public void createIndexTable() {
        try(HBaseAdmin admin = new HBaseAdmin(this.hbaseConf)) {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor(Bytes.toBytes("data")));
            admin.createTable(desc);
        } catch (MasterNotRunningException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void before(String field) {
        this.sc = new JavaSparkContext(DefaultConfLoader.getDefaultConf());
        this.sqlsc = new SQLContext(sc);
        this.dataset = DefaultDataLoader.loadPropleDatasetWithView(this.sqlsc, this.fileName);
        deleteIndex(tableName);
        createIndexTable();
    }

    @Override
    public void createIndex(String field) {
        JavaPairRDD<ImmutableBytesWritable, Put> rdd = dataset.toJavaRDD().mapToPair((Row row) -> {
            Long id = row.getAs("id"); // get the identifier
            int idx_field = row.getAs(field);
            Put put = new Put(Bytes.toBytes(idx_field));
            String name = row.getAs("name");
            String sex = row.getAs("sex");
            int age = row.getAs("age");
            int salary = row.getAs("salary");
            String features = row.getAs("features");
            String json = String.format("{id: %d, name: %s, sex: %s, age: %d, salary: %d, features: %s}", id, name, sex, age, salary, features);
            put.add(Bytes.toBytes("data"), Bytes.toBytes(id), Bytes.toBytes(json));
            return new Tuple2<>(new ImmutableBytesWritable(), put);
        });
        rdd.saveAsHadoopDataset(this.hbaseJobConf);
    }

    @Override
    public void deleteIndex(String field) {
        try(HBaseAdmin admin = new HBaseAdmin(this.hbaseConf)) {
            if(admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } catch (MasterNotRunningException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private Filter getOneFilter(String field, String op, int value) {
        CompareFilter.CompareOp cop;
        switch(op) {
            case "<":
                cop = CompareFilter.CompareOp.LESS;
                break;
            case "<=":
                cop = CompareFilter.CompareOp.LESS_OR_EQUAL;
                break;
            case ">":
                cop = CompareFilter.CompareOp.GREATER;
                break;
            case ">=":
                cop = CompareFilter.CompareOp.GREATER_OR_EQUAL;
                break;
            default:
                cop = CompareFilter.CompareOp.EQUAL;
                break;
        }
        return new RowFilter(cop, new BinaryComparator(Bytes.toBytes(value)));
    }

    private List<Filter> createFilterByCondition(QueryCondition condition) {
        List<Filter> filters = new ArrayList<Filter>();
        String field = condition.getField();
        if(condition.isTypeOne()) {
            filters.add(getOneFilter(field, condition.getOperator(), condition.getValue()));
        } else {
            filters.add(getOneFilter(field, condition.getRightOperator(), condition.getLeftValue()));
            filters.add(getOneFilter(field, condition.getRightOperator(), condition.getRightValue()));
        }
        return filters;
    }

    @Override
    public QueryResult query(QueryCondition condition) {
        List<Long> res = new ArrayList<>();
        try(HTable table = new HTable(this.hbaseConf, tableName)) {
            Scan scan = new Scan();
            FilterList filters = new FilterList(createFilterByCondition(condition));
            scan.setFilter(filters);
            ResultScanner rs = table.getScanner(scan);
            
            for(Result r: rs) {
                for(KeyValue c: r.raw()) {
                    long id = Bytes.toLong(c.getQualifier());
                    res.add(id);
                }
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return new QueryResult(res);
    }

    public static void main(String[] args) {
        HBaseRunner r = new HBaseRunner("out/1MB.json");
        r.before("age");
        r.createIndex("age");
        r.query(new QueryCondition("age < 30"));
        QueryResult res = r.query(new QueryCondition("age < 30"));
        for(Long idx: res) {
            System.out.println(idx);
        }
        r.after("age");
        // r.createIndexTable();
    }

    @Override
    public void after(String field) {
        // TODO Auto-generated method stub
        this.sc.stop();
    }

    
}