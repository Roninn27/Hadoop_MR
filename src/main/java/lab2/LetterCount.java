package lab2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LetterCount {
    //TokenizerMapper类继承了Mapper类并对map函数进行了重写。
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        // 这段代码的意思是把每个单词的数量都置为one，即1，具体什么作用后面执行会体现。
        private final static IntWritable one = new IntWritable(1);
        // 一个变量用来存储传来文件的键值
        private final Text letter = new Text();

        /*
         * 这一段即为具体的map函数，也是关键函数，它在每个节点上执行，
         * 产生<key,list<value>>为后面reduce提供输入，其中Context context就是为reduce保存数据
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*
             * 为分词提供准备，参照StringTokenizer作用。首先将将获得的Text类型的value转成String型
             * 然后再转变成StringTokenizer为后面的分词做准备。
             */
            StringTokenizer itr = new StringTokenizer(value.toString());
            /*
             * while (itr.hasMoreTokens()), 对于一行Text进行扫描，以空格作为分隔符.
             * char c = itr.nextToken().toLowerCase().charAt(0);取首字母并将其改变为小写
             * letter.set(String.valueOf(c));将String型转换成Text型
             * context.write(letter, one);再生成对应的<key,value>对。
             * 比如一行文本hello world hello hadoop（为什么是一行，以设置的InputFormat有关系，本例是默认的，
             * 即TextInputFormat,这个类型是以文本文件中的每一行作为一个记录），该段文本扫描后，能得到的结果是：
             * h 1
             * w 1
             * h 1
             * h 1
             * 为什么全是一，即context.write(letter, one)，中都是以one作为value值的，
             * 得到的输出k/v对为<h ,(1, 1，1)>,<w,1>.
             */
            while (itr.hasMoreTokens()) {
                char c = itr.nextToken().toLowerCase().charAt(0);
                if (c <= 'z' && c >= 'a') {
                    letter.set(String.valueOf(c));
                    context.write(letter, one);
                }
            }
        }
    }

    //IntSumReducer类继承reducer类，并实现reduce函数。
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // 这段代码产生一个result用来存放结果。即<key,value>中的value。
        private final IntWritable result = new IntWritable();

        /*
         * Reduce函数的参数定义，Text key, Iterable<IntWritable> values,即对应map函数传来的<key,list<value>>
         * context同样用来保存数据。*/
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            /*
             * 计算每个key的数量，IntWritable val : values --> 相当于for(Inheritable val=0,val<values.length(),val++)
             * 得出总数保存在sum中，如传入的<h,(1,1，1)>对，sum即等于3.
             */
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            // 将sum转换成IntWritable型，再使用context.write(key, result)-->保存。
            System.out.println("运行了一次");
            result.set(sum);
            context.write(key, result);
        }
    }

    //主函数，mapreduce的入口。
    public static void main(String[] args) throws Exception {
        // 获取配置参数，即前面配置的一些参数，比如core-site.xml，hdfs-site.xml这些里面的一些参数。
        Configuration conf = new Configuration();
        // Job对象，以letter count作为这次job的名字。
        Job job = Job.getInstance(conf, "letter count");
        // Mapreduce的关于map和reduce的设置，job.setCombinerClass(IntSumReducer.class)-->相当于本地的一次reduce，暂不太了解。
        job.setJarByClass(LetterCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        // 输出<k,v>的类型.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 输入输出路径。
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 判断程序是否等待什么的，是否需要退出。
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
