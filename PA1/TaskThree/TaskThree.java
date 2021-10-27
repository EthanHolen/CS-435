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
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class TaskThree {
    public static class TaskThreeMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text text = new Text();

        private final static IntWritable one = new IntWritable(1);

        // @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // ADD

            text.set(value.toString());

            context.write(text, one);

            StringTokenizer tokenizer = new StringTokenizer(value.toString());

            String node_one = tokenizer.nextToken();
            String node_two = tokenizer.nextToken();

            String reverseNodes = node_two + " " + node_one;

            text.set(reverseNodes);

            context.write(text, one);

        }
    }

    public static class TaskThreeReducer extends Reducer<Text, IntWritable, NullWritable, Text> {

        private int stop = 0;

        // @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;

            for (IntWritable val : values) {
                count += val.get();
            }

            if (count > 1 && stop < 100) {
                stop += 1;
                context.write(NullWritable.get(), key);

            }
        }

        // @Override
        // protected void cleanup(Context context) throws IOException,
        // InterruptedException {
        // IntWritable noOfNodes = new IntWritable(distinctNodes.size());
        // context.write(noOfNodes, NullWritable.get());
        // }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "distinct nodes new");
        job.setJarByClass(TaskThree.class);
        job.setMapperClass(TaskThree.TaskThreeMapper.class);
        job.setReducerClass(TaskThree.TaskThreeReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
