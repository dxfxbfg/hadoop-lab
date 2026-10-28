import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    private static final Set<String> STOP_WORDS = new HashSet<String>(Arrays.asList(
            ("a about above after again against all also am an and any are as at be because been "
           + "before being below between both but by can cannot could did do does doing down during "
           + "each few for from further had has have having he her here hers herself him himself his "
           + "how however if in into is it its itself just me more most my myself no nor not of off on "
           + "once one only or other ought our ours ourselves out over own same she should so some such "
           + "than that the their theirs them themselves then there these they this those through to "
           + "too two under until up upon very was we were what when where which while who whom why "
           + "will with would you your yours yourself yourselves").split(" ")));

    public static class IndexMapper extends Mapper<Object, Text, Text, Text> {

        private final Text outKey = new Text();
        private final Text outValue = new Text();
        private String docName;

        @Override
        protected void setup(Context context) {
            docName = ((FileSplit) context.getInputSplit()).getPath().getName();
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().toLowerCase().split("[^a-z]+");
            for (String token : tokens) {
                if (token.length() < 2 || STOP_WORDS.contains(token)) {
                    continue;
                }
                outKey.set(token);
                outValue.set(docName);
                context.write(outKey, outValue);
            }
        }
    }

    public static class IndexCombiner extends Reducer<Text, Text, Text, Text> {

        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> partial = new TreeMap<String, Integer>();
            for (Text value : values) {
                String doc = value.toString();
                Integer old = partial.get(doc);
                partial.put(doc, old == null ? 1 : old + 1);
            }
            outValue.set(join(partial, false));
            context.write(key, outValue);
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {

        private final Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> posting = new TreeMap<String, Integer>();
            for (Text value : values) {
                for (String item : value.toString().split(";")) {
                    int comma = item.lastIndexOf(',');
                    String doc = item.substring(0, comma);
                    int count = Integer.parseInt(item.substring(comma + 1));
                    Integer old = posting.get(doc);
                    posting.put(doc, old == null ? count : old + count);
                }
            }
            outValue.set(join(posting, true));
            context.write(key, outValue);
        }
    }

    private static String join(Map<String, Integer> posting, boolean brackets) {
        StringBuilder sb = new StringBuilder();
        int total = 0;
        for (Map.Entry<String, Integer> entry : posting.entrySet()) {
            if (sb.length() > 0) {
                sb.append(';');
            }
            if (brackets) {
                sb.append('<');
            }
            sb.append(entry.getKey()).append(',').append(entry.getValue());
            if (brackets) {
                sb.append('>');
            }
            total += entry.getValue();
        }
        if (brackets) {
            sb.append(";<total,").append(total).append(">.");
        }
        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(IndexMapper.class);
        job.setCombinerClass(IndexCombiner.class);
        job.setReducerClass(IndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
