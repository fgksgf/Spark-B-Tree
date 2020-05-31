package jhx;

import jhx.bean.QueryCondition;

import java.io.File;

/**
 * @author Huaxi Jiang
 */
public class Main {

    public static void main(String[] args) {
        // 
        final String saveDir = "out";
        final String suffix = ".json";

        // 
        for (int i = 1; i <= 1000; i *= 10) {
            // out/1MB.json
            String readPath = RandomUtil.generateJsonFile(saveDir, i);

            // 
            for (int j = 0; j < 10; j++) {
                QueryCondition qc = RandomUtil.getRandomCondition();

                // out/1MB-age > 20.json
                String resultPath = saveDir + File.separator + i + "MB-" + qc.toString() + suffix;

                RandomUtil.generateQueryResult(readPath, resultPath, qc);
            }
        }
    }
}