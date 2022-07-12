package lab3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class CoTermNSPair {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
            ArrayList<String> termArray = new ArrayList<String>();
            while (itr.hasMoreTokens()){
                termArray.add(itr.nextToken().toLowerCase());
            }
            for (int i = 0; i < termArray.size(); i++) {
                String term1 = termArray.get(i);
                for (int j = i + 1; j < termArray.size(); j++) {
                    String term2 = termArray.get(j);
                    word.set(term1 + " " + term2);
                    context.write(word, one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();
        int count = 1;
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            count ++;
            System.out.println("运行了一次" + count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception{
        args = new String[]{"/Users/ronin/IdeaProjects/Comp9313_Hadoop/src/main/input/tiny-doc.txt",
                "/Users/ronin/IdeaProjects/Comp9313_Hadoop/src/main/output"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "term co-occurrence nonsymmetric pair");
        job.setJarByClass(CoTermNSPair.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
