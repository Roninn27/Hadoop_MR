package comp9313.proj1;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerProj1 extends Partitioner<WritableProj1, NullWritable> {

    @Override
    public int getPartition(WritableProj1 writableProj1, NullWritable nullWritable, int numPartitions) {
        return (writableProj1.getTerm().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
