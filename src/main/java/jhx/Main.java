package jhx;

import jhx.bean.QueryCondition;

import java.io.File;

/**
 * @author Huaxi Jiang
 */
public class Main {

    public static void main(String[] args) {
        // 生成文件存放的目录
        final String saveDir = "out";
        final String suffix = ".json";

        // 随机生成1MB，10MB，100MB，1000MB的json数据文件
        for (int i = 1; i <= 1000; i *= 10) {
            // out/1MB.json
            String readPath = RandomUtil.generateJsonFile(saveDir, i);

            // 随机产生10个查询条件及对应的结果文件
            for (int j = 0; j < 10; j++) {
                QueryCondition qc = RandomUtil.getRandomCondition();

                // out/1MB-age > 20.json
                String resultPath = saveDir + File.separator + i + "MB-" + qc.toString() + suffix;

                RandomUtil.generateQueryResult(readPath, resultPath, qc);
            }
        }
    }
}