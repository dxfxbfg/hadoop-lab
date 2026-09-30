import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GrandParent {

    public static class RelationMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.length() == 0 || line.startsWith("child")) {
                return;
            }
            String[] parts = line.split("\\s+");
            if (parts.length < 2) {
                return;
            }
            String child = parts[0];
            String parent = parts[1];
            context.write(new Text(child), new Text("C:" + parent));
            context.write(new Text(parent), new Text("P:" + child));
        }
    }

    public static class RelationReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> parents = new ArrayList<>();
            List<String> children = new ArrayList<>();

            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("C:")) {
                    parents.add(s.substring(2));
                } else if (s.startsWith("P:")) {
                    children.add(s.substring(2));
                }
            }

            if (parents.isEmpty() || children.isEmpty()) {
                return;
            }
            for (String child : children) {
                for (String parent : parents) {
                    context.write(new Text(child), new Text(parent));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "grand parent mining");
        job.setJarByClass(GrandParent.class);

        job.setMapperClass(RelationMapper.class);
        job.setReducerClass(RelationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
