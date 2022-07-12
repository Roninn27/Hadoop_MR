package lab3.Secondar_Sorting;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparatorDemo extends WritableComparator {
    protected GroupingComparatorDemo() {
        super(WritableDemo.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WritableDemo o1 = (WritableDemo) a;
        WritableDemo o2 = (WritableDemo) b;
        return o1.getYearMonth().compareTo(o2.getYearMonth());
    }
}
