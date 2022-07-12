package comp9313.proj1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparatorProj1 extends WritableComparator {
    protected GroupingComparatorProj1() {
        super(WritableProj1.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WritableProj1 o1 = (WritableProj1) a;
        WritableProj1 o2 = (WritableProj1) b;
        return o1.getTerm().compareTo(o2.getTerm());
    }
}
