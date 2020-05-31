package ytj;

import jhx.bean.Person;
import jhx.bean.QueryCondition;
import ytj.QueryResult;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import java.util.List;
import java.util.ArrayList;
import java.io.FileReader;
import java.lang.reflect.Field;


public abstract class Runner {

    protected final String fileName;

    public Runner(String filename) {
        this.fileName = filename;
    }

    public final String getFileName() {
        return this.fileName;
    }

    public abstract void before(String field);
    public abstract void after(String field);
    public abstract QueryResult query(QueryCondition condition);  // Timing for this method
    

}

