import com.alibaba.fastjson.annotation.JSONField;

import java.util.Comparator;
import java.util.List;

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

class AgeComparator implements Comparator<Person> {
    public int compare(Person p1, Person p2) {
        if (p1.getAge() < p2.getAge()) {
            return -1;
        } else if (p1.getAge() == p2.getAge()) {
            return 0;
        } else {
            return 1;
        }
    }
}

class SalaryComparator implements Comparator<Person> {
    public int compare(Person p1, Person p2) {
        if (p1.getSalary() < p2.getSalary()) {
            return -1;
        } else if (p1.getSalary() == p2.getSalary()) {
            return 0;
        } else {
            return 1;
        }
    }
}