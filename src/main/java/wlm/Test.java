package wlm;

import jhx.RandomUtil;
import ytj.QueryResult;
import jhx.bean.QueryCondition;

public class Test {
    public static void main(String[] args) {
        String fileName = "hdfs://localhost:9000/test_spark/1MB.json";
        QueryWithoutIndexRunner runner = new QueryWithoutIndexRunner(fileName);
        runner.before(fileName);
        QueryCondition queryCondition = RandomUtil.getRandomCondition();
        QueryResult queryResult = runner.query(queryCondition);
    }
}