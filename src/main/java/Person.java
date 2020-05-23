import java.util.List;

import com.alibaba.fastjson.annotation.JSONField;

/**
 * @author Hoshea
 */
public class Person {
    @JSONField(ordinal = 1)
    private String name;

    @JSONField(ordinal = 2)
    private String sex;

    @JSONField(ordinal = 3)
    private int age;

    @JSONField(ordinal = 4)
    private int salary;

    @JSONField(ordinal = 5)
    private List<Float> features;

    private static final int NAME_LEN = 5;

    public Person() {
        this.name = RandomUtil.getRandomString(NAME_LEN);
        this.sex = RandomUtil.getRandomSex();
        this.age = RandomUtil.getRandomAge();
        this.salary = RandomUtil.getRandomSalary();
        this.features = RandomUtil.getRandomFeatures();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    public List<Float> getFeatures() {
        return features;
    }

    public void setFeatures(List<Float> features) {
        this.features = features;
    }
}