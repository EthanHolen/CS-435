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

public class TaskOne {
    public static class EdgeMapper extends Mapper<Object, Text, Text, NullWritable> {

        // @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Text edge = value;
            context.write(edge, NullWritable.get());

        }
    }

    public static class VertexMapper extends Mapper<Object, Text, Text, NullWritable> {

        // @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] split_value = value.toString().split(" ");

            Text vertexOne = new Text(split_value[0]);
            Text vertexTwo = new Text(split_value[1]);

            context.write(vertexOne, NullWritable.get());
            context.write(vertexTwo, NullWritable.get());

        }
    }

    public static class CountReducer extends Reducer<Text, NullWritable, IntWritable, NullWritable> {
        private Set<String> distinct_edges;

        @Override
        protected void setup(Context context) {
            distinct_edges = new HashSet<String>();
        }

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) {
            distinct_edges.add(key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            IntWritable noOfNodes = new IntWritable(distinct_edges.size());
            context.write(noOfNodes, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "distinct nodes new");
        job.setJarByClass(TaskOne.class);
        job.setMapperClass(TaskOne.EdgeMapper.class);
        job.setReducerClass(TaskOne.CountReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        Configuration confTwo = new Configuration();
        Job jobTwo = Job.getInstance(conf, "distinct nodes new");
        jobTwo.setJarByClass(TaskOne.class);
        jobTwo.setMapperClass(TaskOne.VertexMapper.class);
        jobTwo.setReducerClass(TaskOne.CountReducer.class);
        jobTwo.setNumReduceTasks(1);
        jobTwo.setMapOutputKeyClass(Text.class);
        jobTwo.setMapOutputValueClass(NullWritable.class);
        jobTwo.setOutputKeyClass(IntWritable.class);
        jobTwo.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(jobTwo, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobTwo, new Path(args[2]));

        System.exit(jobTwo.waitForCompletion(true) ? 0 : 1);
    }
}
