import java.io.*;
import java.lang.reflect.Field;

import com.alibaba.fastjson.JSON;


/**
 * @author Huaxi Jiang
 */
public class Main {

    /**
     * Save a json string into a json file.
     *
     * @param jsonString
     * @param filePath
     * @param fileName   The file name without postfix.
     * @return If the process success, return true, else return false.
     */
    private static boolean saveJsonFile(String jsonString, String filePath, String fileName) {
        // 标记文件生成是否成功
        boolean flag = true;

        // 拼接文件完整路径
        String fullPath = filePath + File.separator + fileName + ".json";

        // 生成json格式文件
        try {
            // 保证创建一个新文件
            File file = new File(fullPath);
            if (!file.getParentFile().exists()) { // 如果父目录不存在，创建父目录
                file.getParentFile().mkdirs();
            }
            if (file.exists()) { // 如果已存在,删除旧文件
                file.delete();
            }
            file.createNewFile();

            // 将格式化后的字符串写入文件
            Writer write = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
            write.write(jsonString);
            write.flush();
            write.close();
        } catch (Exception e) {
            flag = false;
            e.printStackTrace();
        }

        // 返回是否成功的标记
        return flag;
    }

    /**
     *
     */
    public static void generateJsonFile() {
        for (int i = 1; i <= 1000; i *= 10) {
            saveJsonFile(JSON.toJSONString(new Data(i)), "out", i + "MB");
        }
    }

    public static void main(String[] args) {
//            generateJsonFile();
        saveJsonFile(JSON.toJSONString(new Data(1000)), "out", "1GB");
//        Class<Person> personClass = Person.class;
//        //获取所有的成员变量，包含私有的
//        Field[] fields = personClass.getDeclaredFields();
//        for (int i = 0; i < fields.length; i++) {
//            System.out.println("变量名: " + fields[i].getName());
//        }

//        for (int i = 0; i < 10; i++) {
//            System.out.println(RandomUtil.getRandomCondition());
//        }
//        try {
//            计算生成数据的字节数大小
//            System.out.println(jsonString.getBytes("utf-8").length / 1048576.0);
//        } catch (UnsupportedEncodingException e) {
//            e.printStackTrace();
//        }
    }
}
