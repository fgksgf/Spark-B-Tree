package jhx.bean;

/**
 *
 * @author Huaxi Jiang
 */
public class QueryCondition {
    private String field;
    private String operator;
    private String leftOperator;
    private String rightOperator;
    private int value;
    private int leftValue;
    private int rightValue;
    private String template;
    private boolean isTypeOne;

    public QueryCondition(String con) {
        String[] temp = con.split(" ");
        if (temp.length == 3) {
            this.template = "%s %s %d";
            this.isTypeOne = true;

            this.field = temp[0];
            this.operator = temp[1];
            this.value = Integer.parseInt(temp[2]);
        } else if (temp.length == 5) {
            //this.template = "%d %s %s %s %d";
            this.template = "%s %s %d and %s %s %d";
            this.isTypeOne = false;

            this.leftValue = Integer.parseInt(temp[0]);
            this.leftOperator = temp[1];
            this.field = temp[2];
            this.rightOperator = temp[3];
            this.rightValue = Integer.parseInt(temp[4]);
        }
    }

    public QueryCondition(String field, String operator, int value) {
        // field > v; field >= v; field < v; field <= v; field == v
        this.template = "%s %s %d";
        this.isTypeOne = true;

        this.field = field;
        this.operator = operator;
        this.value = value;
    }

    public QueryCondition(int leftValue, String leftOperator, String field, String rightOperator, int rightValue) {
        // v1 < field < v2; v1 < field <= v2; v1 <= field < v2; v1 <= field <= v2
        //this.template = "%d %s %s %s %d";
        this.template = "%s %s %d and %s %s %d";
        this.isTypeOne = false;

        this.leftValue = leftValue;
        this.leftOperator = leftOperator;
        this.field = field;
        this.rightOperator = rightOperator;
        this.rightValue = rightValue;
    }

    @Override
    public String toString() {
        if (isTypeOne) {
            return String.format(template, field, operator, value);
        } else {
            //return String.format(template, leftValue, leftOperator, field, rightOperator, rightValue);
            return String.format(template, field, leftOperator, leftValue, field, rightOperator, rightValue);
        }
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public String getLeftOperator() {
        return leftOperator;
    }

    public void setLeftOperator(String leftOperator) {
        this.leftOperator = leftOperator;
    }

    public String getRightOperator() {
        return rightOperator;
    }

    public void setRightOperator(String rightOperator) {
        this.rightOperator = rightOperator;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getLeftValue() {
        return leftValue;
    }

    public void setLeftValue(int leftValue) {
        this.leftValue = leftValue;
    }

    public int getRightValue() {
        return rightValue;
    }

    public void setRightValue(int rightValue) {
        this.rightValue = rightValue;
    }

    public boolean isTypeOne() {
        return isTypeOne;
    }

    public void setTypeOne(boolean typeOne) {
        isTypeOne = typeOne;
    }
}