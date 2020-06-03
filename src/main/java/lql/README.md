## Spark implementation of B+-Tree Indexing

### Quick Usage
```
$ export MAVEN_OPTS="-Xms256m -Xmx768m -XX:PermSize=128m -XX:MaxPermSize=256M"
$ mvn compile
$ mvn exec:java -Dexec.mainClass="lql.SparkBTIndex"
```

### Class

- Address
    
    The Java Bean class for parsing address(int offset, int length).
- PartitionsMapper
    
    The class implements FlatMapFunction<Iterator<int[]>, String> for mapping in query.
- SparkBTIndex

    The class of our approach, which extends Runner and implements Indexable, Serializable.
    And contains a main function for testing.
