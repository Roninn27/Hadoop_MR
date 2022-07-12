package comp9313.proj1;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Project1 {

    public static class TokenizerMapper extends Mapper<Object, Text, WritableProj1, NullWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",|，|\\s+");
            for (int i = 1; i < values.length; i++) {
//                System.out.println(new WritableProj1(values[i], -1, values[0].substring(0, 4)));
                context.write(new WritableProj1(values[i], -1, values[0].substring(0, 4)), NullWritable.get());
//                System.out.println(new WritableProj1(values[i], 1, values[0].substring(0, 4)));
                context.write(new WritableProj1(values[i], 1, values[0].substring(0, 4)), NullWritable.get());
            }
        }
    }

    public static class IntSumReducer extends Reducer<WritableProj1, NullWritable, Text, Text> {
        private final HashMap<String, String> reduceMap = new HashMap<String, String>();
        private int termTF = 0;
        private String flagYear = "9999";
        private int demoLen = 0;
        private HashSet<String> yearSet = new HashSet<>();

        @Override
        protected void reduce(WritableProj1 key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int numDoc = Integer.parseInt(conf.get("numDoc"));
            StringBuilder val = new StringBuilder();
            for (NullWritable value : values) {
                if (key.getSpecialKey() == -1) {
                    demoLen++;
                    flagYear = "9999";
                    yearSet.add(key.getYear());
                } else {
                    demoLen--;
                    if (flagYear.compareTo(key.getYear()) != 0) {
                        if (flagYear.compareTo("9999") != 0) {
//                            BigDecimal weight = new BigDecimal(termTF * Math.log10(numDoc / (double) yearSet.size()));
//                            val.append(flagYear).append(',').append(weight.setScale(16, RoundingMode.HALF_UP)).append(";");
                            double weight = termTF * Math.log10((double) numDoc / (double) yearSet.size());
                            val.append(flagYear).append(',').append(weight).append(";");
                        }
                        flagYear = key.getYear();
                        termTF = 1;
                    } else {
                        termTF++;
                    }
                    if (demoLen == 0) {
//                        BigDecimal weight = new BigDecimal(termTF * Math.log10(numDoc / (double) yearSet.size()));
//                        val.append(key.getYear()).append(',').append(weight.setScale(16, RoundingMode.HALF_UP));
                        double weight = termTF * Math.log10((double) numDoc / (double) yearSet.size());
                        val.append(flagYear).append(',').append(weight);
                        yearSet = new HashSet<>();
                    }
                }
            }
            context.write(new Text(key.getTerm()), new Text(String.valueOf(val)));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("numDoc", args[2]);
        Job job = Job.getInstance(conf, "Comp9313 Project1");
        job.setJarByClass(Project1.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setNumReduceTasks(Integer.parseInt(args[3]));
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(WritableProj1.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setPartitionerClass(PartitionerProj1.class);
        job.setGroupingComparatorClass(GroupingComparatorProj1.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}