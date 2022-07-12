package lab3.Secondar_Sorting;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerDemo extends Partitioner<WritableDemo, NullWritable> {

    @Override
    public int getPartition(WritableDemo writableMapDemo, NullWritable nullWritable, int i) {
        return(writableMapDemo.getYearMonth().hashCode() & Integer.MAX_VALUE) % i;
    }
}
