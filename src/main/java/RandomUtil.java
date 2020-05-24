import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 随机生成工具类
 *
 * @author Hoshea
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
            v2 = getRandomInt(v1 + 1, MAX_AGE);
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
}
