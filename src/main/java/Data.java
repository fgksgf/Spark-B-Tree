import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Data {
    private static final int COUNT_PER_MB = 8400;
    private List<Person> people = new ArrayList<Person>();

    /**
     * 生成指定大小的数据，单位为MB
     *
     * @param mb
     */
    public Data(int mb) {
        for (int i = 0; i < mb * COUNT_PER_MB; i++) {
            this.people.add(new Person());
        }
    }

    public List<Person> getPeople() {
        return people;
    }

    public void setPeople(List<Person> people) {
        this.people = people;
    }

    public List<Person> query(QueryCondition qc) {
        List<Person> ret = new ArrayList<Person>();

        if (qc.getField().equals("age")) {
            Collections.sort(people, new AgeComparator());
        } else if (qc.getField().equals("salary")) {
            Collections.sort(people, new SalaryComparator());
        }

        if (qc.isTypeOne()) {
            for (int i = 0; i < people.size(); i++) {

            }
        } else {

        }

        return ret;
    }
}
