package wlm;

import jhx.RandomUtil;
import ytj.QueryResult;
import jhx.bean.QueryCondition;

public class TestMyRunner {
    public static void main(String[] args) {
        String fileName = "out/1MB.json";
        MyRunner runner = new MyRunner(fileName);
        QueryCondition condition = RandomUtil.getRandomCondition();
        QueryResult test = runner.queryWithoutIndex(condition);
        
        
    }
}