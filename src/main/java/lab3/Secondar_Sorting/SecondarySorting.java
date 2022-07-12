package lab3.Secondar_Sorting;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class SecondarySorting {

    public static class TokenizerMapper extends Mapper<Object, Text, WritableDemo, NullWritable> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, WritableDemo, NullWritable>.Context context)
                throws IOException, InterruptedException {
            String[] values = value.toString().split(" ");
            context.write(new WritableDemo(values[0] + '-' + values[1],
                    Integer.parseInt(values[2])), NullWritable.get());
        }
    }


    public static class IntSumReducer extends Reducer<WritableDemo, NullWritable, Text, Text> {
        @Override
        protected void reduce(WritableDemo key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            StringBuilder val = new StringBuilder();
            for (NullWritable value : values) {
                val.append(" ").append(key.getTemperature());
            }
            context.write(new Text(key.getYearMonth()), new Text(String.valueOf(val)));
        }
    }


    public static void main(String[] args) throws Exception {
        args = new String[]{"/Users/ronin/IdeaProjects/Comp9313_Hadoop/src/main/input/input_test.txt",
                "/Users/ronin/IdeaProjects/Comp9313_Hadoop/src/main/output"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Secondary Sorting");
        job.setJarByClass(SecondarySorting.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setNumReduceTasks(10);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(WritableDemo.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(PartitionerDemo.class);
        job.setGroupingComparatorClass(GroupingComparatorDemo.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}