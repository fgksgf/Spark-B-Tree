package ytj;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Iterator;
import java.util.List;

public class QueryResult implements Iterable<Long> {

    private List<Long> id;

    public QueryResult(List<Long> id) {
        this.id = id;
    }

    public List<Long> getAll() {
        return this.id;
    }

    @Override
    public Iterator<Long> iterator() {
        return id.iterator();
    }
    
}