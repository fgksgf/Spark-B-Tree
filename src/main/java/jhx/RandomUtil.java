package jhx;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import jhx.bean.Person;
import jhx.bean.QueryCondition;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 随机生成工具类
 *
 * @author Huaxi Jiang
 */
public class RandomUtil {
    private static final int SEED = 42;

    private static final int MIN_AGE = 18;
    private static final int MAX_AGE = 70;
    private static final int SALARY_STEP = 100;
    private static final int MIN_SALARY = 1000;
    private static final int MAX_SALARY = 50000;
    private static final int FEATURE_NUM = 5;
    private static final Random RANDOM = new Random(SEED);

    // 每1MB数据对应的大致数据条数
    private static final int COUNT_PER_MB = 8300;

    private static final String[] OPERATORS = {">", ">=", "<", "<=", "=="};
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    /**
     * 随机产生一个[left, right]范围内的整数
     *
     * @param left  左边界，闭区间
     * @param right 右边界，闭区间
     * @return 随机整数
     */
    private static int getRandomInt(int left, int right) {
        int ret = 0;
        if (left <= right) {
            ret = RANDOM.nextInt(right) % (right - left + 1) + left;
        }
        return ret;
    }

    /**
     * 随机产生一个指定长度、仅包含字母的字符串
     *
     * @param len 字符串长度
     * @return 随机字符串
     */
    public static String getRandomString(int len) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < len; i++) {
            int number = RANDOM.nextInt(ALPHABET.length());
            sb.append(ALPHABET.charAt(number));
        }
        return sb.toString();
    }

    /**
     * 随机产生一个性别字符串
     *
     * @return "male"或者"female"
     */
    public static String getRandomSex() {
        String ret = null;
        if (RANDOM.nextBoolean()) {
            ret = "male";
        } else {
            ret = "female";
        }
        return ret;
    }

    /**
     * 随机产生一个 [MIN_AGE, MAX_AGE] 范围内的整数年龄
     *
     * @return 随机整数年龄
     */
    public static int getRandomAge() {
        return getRandomInt(MIN_AGE, MAX_AGE);
    }

    /**
     * 随机产生一个 [MIN_SALARY, MAX_SALARY] 范围内的整数工资
     *
     * @return 随机整数工资
     */
    public static int getRandomSalary() {
        return getRandomInt(MIN_SALARY / SALARY_STEP, MAX_SALARY / SALARY_STEP) * SALARY_STEP;
    }

    /**
     * 随机产生包含 FEATURE_NUM 个随机浮点数的列表
     *
     * @return 元素为浮点数的列表
     */
    public static List<Float> getRandomFeatures() {
        List<Float> ret = new ArrayList<Float>();
        for (int i = 0; i < FEATURE_NUM; i++) {
            ret.add(RANDOM.nextFloat());
        }
        return ret;
    }

    /**
     * 随机生成一个查询条件
     *
     * @return 随机查询条件
     */
    public static QueryCondition getRandomCondition() {
        QueryCondition ret;
        String field;
        int v, v1, v2;

        if (RANDOM.nextBoolean()) {
            field = "age";
            v = getRandomAge();
            v1 = getRandomInt(MIN_AGE, MAX_AGE - 2);
            v2 = getRandomInt(v1 + 2, MAX_AGE);
        } else {
            field = "salary";
            v = getRandomSalary();
            v1 = getRandomSalary() - SALARY_STEP;
            v2 = getRandomInt(v1 / SALARY_STEP, MAX_SALARY / SALARY_STEP) * SALARY_STEP;
        }

        if (RANDOM.nextBoolean()) {
            int type = RANDOM.nextInt(100) % 5;
            ret = new QueryCondition(field, OPERATORS[type], v);
        } else {
            int leftType = getRandomInt(2, 3);
            int rightType = getRandomInt(2, 3);
            ret = new QueryCondition(v1, OPERATORS[leftType], field, OPERATORS[rightType], v2);
        }
        return ret;
    }

    /**
     * 在指定目录下随机生成一个指定大小的json数据文件
     *
     * @param saveDir 文件保存路径
     * @param mb      文件大小，单位为MB
     * @return 生成文件的完整路径
     */
    public static String generateJsonFile(String saveDir, int mb) {
        Gson gson = new Gson();
        String fileName = mb + "MB.json";
        String fullPath = saveDir + File.separator + fileName;

        try {
            JsonWriter writer = new JsonWriter(new FileWriter(fullPath));
            writer.beginArray();
            for (int i = 0; i < COUNT_PER_MB * mb; ++i) {
                gson.toJson(new Person(), Person.class, writer);
            }
            writer.endArray();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed: generate " + fileName);
        }
        System.out.println("Done: generate" + fileName);
        return fullPath;
    }

    /**
     * 从指定路径读取json文件进行查询，将符合条件的结果保存为json文件
     *
     * @param readPath  读取文件完整路径
     * @param writePath 保存结果文件的完整路径
     * @param qc        查询条件
     */
    public static void generateQueryResult(String readPath, String writePath, QueryCondition qc) {
        Gson gson = new Gson();
        Field field = null;
        try {
            field = Person.class.getDeclaredField(qc.getField());
            field.setAccessible(true);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
            System.out.println("Failed: generate result file " + writePath);
            return;
        }

        try {
            JsonReader reader = new JsonReader(new FileReader(readPath));
            JsonWriter writer = new JsonWriter(new FileWriter(writePath));

            reader.beginArray();
            writer.beginArray();
            while (reader.hasNext()) {
                Person p = gson.fromJson(reader, Person.class);
                boolean flag = false;

                if (qc.isTypeOne()) {
                    int v = (int) field.get(p);
                    switch (qc.getOperator()) {
                        case "<":
                            if (v < qc.getValue()) {
                                flag = true;
                            }
                            break;
                        case "<=":
                            if (v <= qc.getValue()) {
                                flag = true;
                            }
                            break;
                        case ">":
                            if (v > qc.getValue()) {
                                flag = true;
                            }
                            break;
                        case ">=":
                            if (v >= qc.getValue()) {
                                flag = true;
                            }
                            break;
                        case "==":
                            if (v == qc.getValue()) {
                                flag = true;
                            }
                            break;
                    }
                } else {
                    int v = (int) field.get(p);
                    String leftOp = qc.getLeftOperator();
                    String rightOp = qc.getRightOperator();

                    if (leftOp.equals("<") && rightOp.equals("<")) {
                        if (qc.getLeftValue() < v && v < qc.getRightValue()) {
                            flag = true;
                        }
                    } else if (leftOp.equals("<") && rightOp.equals("<=")) {
                        if (qc.getLeftValue() < v && v <= qc.getRightValue()) {
                            flag = true;
                        }
                    } else if (leftOp.equals("<=") && rightOp.equals("<=")) {
                        if (qc.getLeftValue() <= v && v <= qc.getRightValue()) {
                            flag = true;
                        }
                    } else if (leftOp.equals("<=") && rightOp.equals("<")) {
                        if (qc.getLeftValue() <= v && v < qc.getRightValue()) {
                            flag = true;
                        }
                    }
                }
                if (flag) {
                    gson.toJson(p, Person.class, writer);
                }
            }
            reader.endArray();
            reader.close();
            writer.endArray();
            writer.close();
        } catch (IOException | IllegalAccessException e) {
            e.printStackTrace();
            System.out.println("Failed: generate result file " + writePath);
        }
        System.out.println("Done: generate result file " + writePath);
    }
}
