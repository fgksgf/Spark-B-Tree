import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Hoshea
 */
public class Data {
    // 每1MB数据对应的大致数据条数
    private static final int COUNT_PER_MB = 8400;
    private List<Person> people;

    /**
     * 生成指定大小的数据，单位为MB
     *
     * @param mb 要生成多大的数据
     */
    public Data(int mb) {
        this.people = new ArrayList<>();
        for (int i = 0; i < mb * COUNT_PER_MB; i++) {
            this.people.add(new Person());
        }
    }

    public Data() {}

    public List<Person> getPeople() {
        return people;
    }

    public void setPeople(List<Person> people) {
        this.people = people;
    }

    /**
     * 根据所给的查询条件筛选元素
     *
     * @param qc 查询条件
     * @return 符合条件的元素列表
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    public List<Person> query(QueryCondition qc) throws NoSuchFieldException, IllegalAccessException {
        List<Person> ret = new ArrayList<>();

        Field field = Person.class.getDeclaredField(qc.getField());
        field.setAccessible(true);

        if (qc.isTypeOne()) {
            for (Person p : people) {
                int v = (int) field.get(p);
                switch (qc.getOperator()) {
                    case "<":
                        if (v < qc.getValue()) {
                            ret.add(p);
                        }
                        break;
                    case "<=":
                        if (v <= qc.getValue()) {
                            ret.add(p);
                        }
                        break;
                    case ">":
                        if (v > qc.getValue()) {
                            ret.add(p);
                        }
                        break;
                    case ">=":
                        if (v >= qc.getValue()) {
                            ret.add(p);
                        }
                        break;
                    case "==":
                        if (v == qc.getValue()) {
                            ret.add(p);
                        }
                        break;
                }
            }
        } else {
            for (Person p : people) {
                int v = (int) field.get(p);
                String leftOp = qc.getLeftOperator();
                String rightOp = qc.getRightOperator();

                if (leftOp.equals("<") && rightOp.equals("<")) {
                    if (qc.getLeftValue() < v && v < qc.getRightValue()) {
                        ret.add(p);
                    }
                } else if (leftOp.equals("<") && rightOp.equals("<=")) {
                    if (qc.getLeftValue() < v && v <= qc.getRightValue()) {
                        ret.add(p);
                    }
                } else if (leftOp.equals("<=") && rightOp.equals("<=")) {
                    if (qc.getLeftValue() <= v && v <= qc.getRightValue()) {
                        ret.add(p);
                    }
                } else if (leftOp.equals("<=") && rightOp.equals("<")) {
                    if (qc.getLeftValue() <= v && v < qc.getRightValue()) {
                        ret.add(p);
                    }
                }
            }
        }

        return ret;
    }
}
