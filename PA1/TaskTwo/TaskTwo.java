import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// import jdk.internal.jshell.tool.resources.l10n;

import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.mapred.Reporter;

public class TaskTwo {
    public static class inDegreeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text userA = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException {
            // StringTokenizer itr = new StringTokenizer(value.toString());
            // String hold = itr.nextToken();
            // userA.set(itr.nextToken());

            // try {
            // context.write(userA, one);
            // } catch (InterruptedException e) {
            // }

            String line = value.toString();
            String[] line_spl = line.split(" ");

            String node1 = line_spl[0];
            String node2 = line_spl[1];

            Text node1_txt = new Text(node1);
            Text node2_txt = new Text(node2);

            try {
                context.write(node2_txt, one);
            } catch (Exception e) {

            }

        }
    }

    public static class outDegreeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text userA = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException {
            // StringTokenizer itr = new StringTokenizer(value.toString());
            // userA.set(itr.nextToken());
            // try {
            // context.write(userA, one);
            // } catch (Exception e) {
            // }
            String line = value.toString();
            String[] line_spl = line.split(" ");

            String node1 = line_spl[0];
            String node2 = line_spl[1];

            Text node1_txt = new Text(node1);
            Text node2_txt = new Text(node2);

            try {
                context.write(node1_txt, one);
            } catch (Exception e) {

            }

        }
    }

    public static class TopKMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        Integer k = 100;

        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException {

            String line = value.toString();
            String[] line_spl = line.split("\t");

            // System.out.println("VALUE: " + line);

            String node_id = line_spl[0];
            String degrees = line_spl[1];

            repToRecordMap.put(Integer.parseInt(degrees), new Text(node_id));
            // System.out.println("CURRENTMAP: " + repToRecordMap);

            if (repToRecordMap.size() > k) {

                repToRecordMap.remove(repToRecordMap.firstKey());

            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            for (Text t : repToRecordMap.values()) {

                // System.out.println("t: " + t);

                context.write(NullWritable.get(), (t.getKey() + " " + t));

            }

        }
    }

    public static class DegreeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            try {
                context.write(key, new IntWritable(count));
            } catch (Exception e) {
            }
        }
    }

    public static class TopKReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        Integer k = 100;
        private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException {

            for (Text value : values) {

                // System.out.println("REDVALUE: " + value);

                String line = value.toString();
                // String[] line_spl = line.split("\t");

                repToRecordMap.put(Integer.parseInt(value), new Text(value));

                if (repToRecordMap.size() > k) {

                    repToRecordMap.remove(repToRecordMap.firstKey());

                }

            }

            for (Text t : repToRecordMap.descendingMap().values()) {

                try {
                    // System.out.println("text: " + t);
                    context.write(NullWritable.get(), t);

                } catch (Exception e) {
                    // TODO: handle exception
                    System.out.println(e);
                }

            }

        }
    }

    public static void main(String[] args) throws Exception {
        // MAP 1
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "in degree new");
        job.setJarByClass(TaskTwo.class);
        job.setMapperClass(inDegreeMapper.class);
        job.setReducerClass(DegreeReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // WAIT FOR MAPPER 2
        job.waitForCompletion(true);

        // MAP #2
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "out degree new");
        job2.setJarByClass(TaskTwo.class);
        job2.setMapperClass(outDegreeMapper.class);
        job2.setReducerClass(DegreeReducer.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // GET FILES AND OUTPUT FILE
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        // WAIT FOR MAPPER3
        job2.waitForCompletion(true);

        // MAP #3
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "secondary processing for indegree");
        job3.setJarByClass(TaskTwo.class);
        job3.setMapperClass(TopKMapper.class);
        job3.setReducerClass(TopKReducer.class);
        job3.setNumReduceTasks(1);
        job3.setOutputKeyClass(NullWritable.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(args[1]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        // WAIT FOR MAPPER3
        job3.waitForCompletion(true);

        // MAP #4
        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4, "secondary processing for outdegree");
        job4.setJarByClass(TaskTwo.class);
        job4.setMapperClass(TopKMapper.class);
        job4.setReducerClass(TopKReducer.class);
        job4.setNumReduceTasks(1);
        job4.setOutputKeyClass(NullWritable.class);
        job4.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job4, new Path(args[2]));
        FileOutputFormat.setOutputPath(job4, new Path(args[4]));
        // GET FILES AND OUTPUT FILE
        // FileInputFormat.addInputPath(job3, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        job4.waitForCompletion(true);

        // FINISH WHEN LAST MAPPER IS DONE
        // System.exit(job2.waitForCompletion(true) ? 0 : 1);
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}