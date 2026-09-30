import java.io.IOException;

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

public class NumberSort {

    public static class SortMapper extends Mapper<Object, Text, IntWritable, NullWritable> {
        private final IntWritable num = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String s = value.toString().trim();
            if (s.length() > 0) {
                num.set(Integer.parseInt(s));
                context.write(num, NullWritable.get());
            }
        }
    }

    public static class SortReducer extends Reducer<IntWritable, NullWritable, Text, IntWritable> {
        private int rank = 0;

        @Override
        public void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            rank++;
            context.write(new Text(rank + " "), key);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "number sort");
        job.setJarByClass(NumberSort.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
