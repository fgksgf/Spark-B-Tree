import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Hoshea
 */
public class Data {
    private static final int COUNT_PER_MB = 8400;
    private List<Person> people;

    /**
     * 生成指定大小的数据，单位为MB
     *
     * @param mb
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
