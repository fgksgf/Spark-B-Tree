package ytj;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.SQLContext;


public class DefaultDataLoader {

    public static final Dataset<Row> loadPropleDataset(SQLContext sqlContext, String filename, boolean createView) {
        StructType schema = new StructType().add("id", DataTypes.LongType)
                .add("age", DataTypes.IntegerType).add("salary", DataTypes.IntegerType)
                .add("sex", DataTypes.StringType).add("name", DataTypes.StringType)
                .add("features", DataTypes.StringType);
        Dataset<Row> dataset = sqlContext.jsonFile(filename, schema);
        if(createView) {
            dataset.createOrReplaceTempView("people");
        }
        return dataset;
    }

    public static final Dataset<Row> loadPropleDatasetWithView(SQLContext sqlContext, String filename) {
        return loadPropleDataset(sqlContext, filename, true);
    }
    
}