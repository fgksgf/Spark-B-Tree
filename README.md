# Spark 和B+-Tree

## 作业要求

### 目标

对于HDFS上存储的数据生成B+-Tree索引并进一步利用B+-Tree索引

### 步骤

#### 1.建立索引
+ 输入:HDFS输入数据(JSON格式)，建立索引的域(例如a.b.c) 
+ 用Spark计算并在HDFS上生成文件对应输入的有序索引

#### 2.使用索引
+ 在Spark加载数据时，可以对索引的域给出过滤条件
+ 加载过程中利用索引完成过滤，仅把满足条件的数据放入RDD
􏰀
### 讨论

除了上述场景外，在Spark中还有什么方式可能使用索引?或者不可能使用索引?为什么?

## Proposal

### 假设

1.  对于json只考虑行式存储，如下例所示:

```json
[
{"name": "a", "number": 12345, "feature": [1.01, 2.35, 7.48, 2.4]},
{"name": "b", "number": 1024, "feature": [2.35, 3.28, 6.44, 3.87]}
]
```
2.  查询条件仅仅限于：`>`，`<`，`==`，`>=`，`<=`；可以有双边，如`1 < field <= 3`

### 主要功能

1. 建立索引：`/create_index.sh -i[input_json_file_name] -f[json_path]`
建立索引后，需要将索引文件写入`hdfs`（一个`field`一个索引），同时刷新索引的`metadata`文件。文件为`[input_json_file_name].keymeta`，里面存放的是(`jsonpath`与索引文件的`path`的映射)。

如：`./create_index.sh -i sample.json -f .number `
然后程序会输出索引文件`sample1.key`，然后追加一个kv对`.number -> sample1.key`

2. 索引的查询(condition 里面包含了字段和条件)：`./search _index.sh -c[condition] input_json_file_name`
程序进入`[input_json_file_name].keymeta`，通过`input_json_file_name`和`condition`里面指定的`jsonpath`，定位到索引文件，然后通过`condition`中的筛选条件，进行筛选，将筛选结果写入`hdfs`，
格式与输入的`json`相同。

如：`./search_location_index.sh -c".number<10" sample.json`

3. 实验与对比：使用索引与不使用索引相比，性能的提升（编写测试）。横坐标为数据量，纵坐标为查询时间，画两条线表示趋势。这就需要朴素算法实现1与2中相同的功能。

### 实验安排
1. 测试用例的构建，包括json，查询条件和预期结果
2. 开发b+树索引
3.  baseline程序的编写
4. 测试程序的编写，包括性能度量、画出b+树索引对于效果的提升
5. 研究b+树索引的其他场景
