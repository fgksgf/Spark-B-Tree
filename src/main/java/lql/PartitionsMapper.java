package lql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;

public class PartitionsMapper implements FlatMapFunction<Iterator<int[]>, String> {

    public String fileName;

    public PartitionsMapper(String fileName) {
        this.fileName = fileName;
    }

    @Override
    public Iterator<String> call(Iterator<int[]> iterator) throws Exception {
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataInputStream in_stream = fs.open(new Path(fileName));
        ArrayList<String> res = new ArrayList<>();
        while (iterator.hasNext()){
            int[] n = iterator.next();
            int offset = n[0], length = n[1];
            // load content
            byte[] buffer = new byte[length];
            in_stream.read(offset, buffer, 0, length);
            String json = new String(buffer);
            res.add(json);
        }
        fs.close();
        in_stream.close();
        return res.iterator();
    }
}
