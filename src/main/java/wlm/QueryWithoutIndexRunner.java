package wlm;

import ytj.Runner;
import jhx.bean.Person;
import ytj.QueryResult;
import jhx.bean.QueryCondition;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;


import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Field;

/**
 * 无索引查询类
 * 
 * @author 王立明
 */

public class QueryWithoutIndexRunner extends Runner {

    private Dataset<Row> dataSet;
    private SparkSession spark;

    /**
     * @param filename 文件的hdfs目录
     */
    public QueryWithoutIndexRunner(String filename) {
        super(filename);
    }

    /**
     * @param field 未用到，调用时随便给一个参数，为空会报错。
     */
    @Override
    public void before(String field) {
        spark = SparkSession
                        .builder()
                        .appName("json to dataset")
                        .master("local")
                        .config("spark.testing.memory", "2147480000")
                        .getOrCreate();
        this.dataSet = spark.read().json(this.fileName);
        this.dataSet.createOrReplaceTempView("person");
    }

    @Override
    public void after(String field) {}

    /**
     * 本类的主要功能函数
     * @param condition 查询条件
     * @return ytj包中定义的QueryResult  
     */
    @Override
    public QueryResult query(QueryCondition condition) {

        //Convert query condition to sql.
        String sqlQuery = null;
        String field = condition.getField();
        String sqlPrefix = "SELECT id FROM person WHERE ";
        if (condition.isTypeOne()) {
            String op = condition.getOperator();
            if (op == "==") op = "=";
            sqlQuery = sqlPrefix + field + " " + op + " " + condition.getValue();
        } else {
            String leftOp = condition.getLeftOperator();
            if (leftOp == "<") leftOp = ">="; 
            else leftOp = ">";
            sqlQuery = sqlPrefix + field + " " + leftOp + " "  + condition.getLeftValue()
                    + " AND " + field + " " + condition.getRightOperator() + " "  + condition.getRightValue(); 
        }

        //run query.
        Dataset<Row> idDF = spark.sql(sqlQuery);
        List<Long> result = idDF.map(row -> Long.valueOf(row.mkString()), Encoders.LONG()).collectAsList();

        //Close spark
        spark.close();

        return new QueryResult(result);
    }
}