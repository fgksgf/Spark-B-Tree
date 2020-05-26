package ytj;

import jhx.bean.Person;
import jhx.bean.QueryCondition;
import java.util.List;



public abstract class Runner {

    private final String fileName;

    public Runner(String filename) {
        this.fileName = filename;
    }

    public final String getFileName() {
        return this.fileName;
    }

    public abstract void beforeCreateIndex(String field);

    public abstract void createIndex(String field);  // Timing for this method

    public abstract void afterCreateIndex(String field);

    public abstract void beforeDeleteIndex(String field);

    public abstract void deleteIndex(String field);  // Timing for this method

    public abstract void afterDeleteIndex(String field);

    public QueryResult queryWithoutIndex(QueryCondition condition) {
        return null;  // TODO: Add query code here
    }

    public abstract QueryResult queryWithIndex(QueryCondition condition);  // Timing for this method

}

