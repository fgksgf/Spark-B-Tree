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

    // 
    private static final int COUNT_PER_MB = 8300;

    private static final String[] OPERATORS = {">", ">=", "<", "<=", "=="};
    private static final String ALPHABET = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    /**
     * 
     */
    private static int getRandomInt(int left, int right) {
        int ret = 0;
        if (left <= right) {
            ret = RANDOM.nextInt(right) % (right - left + 1) + left;
        }
        return ret;
    }

    /**
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
     */
    public static int getRandomAge() {
        return getRandomInt(MIN_AGE, MAX_AGE);
    }

    /**
     */
    public static int getRandomSalary() {
        return getRandomInt(MIN_SALARY / SALARY_STEP, MAX_SALARY / SALARY_STEP) * SALARY_STEP;
    }

    /**
     */
    public static List<Float> getRandomFeatures() {
        List<Float> ret = new ArrayList<Float>();
        for (int i = 0; i < FEATURE_NUM; i++) {
            ret.add(RANDOM.nextFloat());
        }
        return ret;
    }

    /**
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
     */
    public static String generateJsonFile(String saveDir, int mb) {
        Gson gson = new Gson();
        String fileName = mb + "MB.json";
        String fullPath = saveDir + File.separator + fileName;

        try {
            JsonWriter writer = new JsonWriter(new FileWriter(fullPath));
            writer.beginArray();
            for (long i = 1; i <= COUNT_PER_MB * mb; ++i) {
                gson.toJson(new Person(), Person.class, writer);
            }
            writer.endArray();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed: generate " + fileName);
        }
        System.out.println("Done: generate " + fileName);
        return fullPath;
    }

    /**
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

                // 
                if (flag) {
                    gson.toJson(p.getId(), Long.class, writer);
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
