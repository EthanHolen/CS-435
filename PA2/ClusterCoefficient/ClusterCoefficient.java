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

import org.apache.hadoop.filecache.DistributedCache;
import java.net.URI;
import java.net.URISyntaxException;

import java.awt.List;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;

import javax.naming.Context;

public class ClusterCoefficient {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        // private Text node;

        // @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // node = value;

            String line = value.toString();

            String[] split_line = line.split(" ");

            String first_node = split_line[0];
            String second_node = split_line[1];

            context.write(new Text(first_node), new Text(second_node));
            context.write(new Text(second_node), new Text(first_node));

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String output = "";

            output += key.toString() + "\t";

            for (Text txt : values) {
                output += txt + " ";
            }

            context.write(new Text(output), NullWritable.get());

        }

    }

    public static class LengthTwoMap extends Mapper<Object, Text, Text, Text> {
        // private Text node;

        // @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // node = value;

            // URI[] files = context.getCacheFiles();

            // System.out.println("CACHE_FILE" + files[0]);

            String line = value.toString();

            String[] split_line = line.split("\t");

            String first_node = split_line[0];

            String[] adjacent_nodes = split_line[1].split(" ");
            // System.out.println("LN: " + split_line[1]);

            if (adjacent_nodes.length >= 2) {

                for (int i = 0; i < adjacent_nodes.length; i++) {

                    for (int j = i; j < adjacent_nodes.length; j++) {

                        if (i != j) {

                            if (adjacent_nodes[i].compareTo(adjacent_nodes[j]) > 0) {
                                String start_end = adjacent_nodes[j] + " " + adjacent_nodes[i];
                                String path = adjacent_nodes[j] + "~" + first_node + "~" + adjacent_nodes[i];
                                context.write(new Text(start_end), new Text(path));
                            } else {
                                String start_end = adjacent_nodes[i] + " " + adjacent_nodes[j];
                                String path = adjacent_nodes[i] + "~" + first_node + "~" + adjacent_nodes[j];
                                context.write(new Text(start_end), new Text(path));
                            }

                        }

                    }

                }

            }

        }
    }

    public static class LengthTwoReduce extends Reducer<Text, Text, Text, Text> {

        // private Properties adjacencyProp = new Properties();

        // @Override
        // protected void setup(Context context) throws IOException,
        // InterruptedException {

        // InputStream iStream = new FileInputStream("./part-r-00000");

        // adjacencyProp.load(iStream);

        // }

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // String aContents = adjacencyProp.getProperty("a");

            // System.out.println("A-Contents: " + aContents);

            String output = "";

            ArrayList<String> vals = new ArrayList<String>();

            for (Text txt : values) {

                vals.add(txt.toString());

            }

            if (vals.size() > 1) {

                output = vals.get(0);

            } else {
                output = vals.get(0);
            }

            context.write(key, new Text(output));

        }

    }

    public static class IdentifyTrianglesMap extends Mapper<Object, Text, Text, Text> {
        // private Text node;

        // @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // node = value;

            // URI[] files = context.getCacheFiles();

            // System.out.println("CACHE_FILE" + files[0]);

            String line = value.toString();

            String[] split_line = line.split("\t");

            context.write(new Text(split_line[0]), new Text(split_line[1]));

        }
    }

    public static class IdentifyTrianglesReduce extends Reducer<Text, Text, Text, Text> {

        private Properties adjacencyProp = new Properties();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            InputStream iStream = new FileInputStream("./part-r-00000");

            adjacencyProp.load(iStream);

        }

        protected void reduce(Text key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {

            String[] splitkey = key.toString().split(" ");

            String node_a = splitkey[0];
            String node_b = splitkey[1];

            // System.out.println("Node a: " + node_a + " Node b: " + node_b);

            // for (Text val : values) {
            // System.out.println(key + " VAL: " + val);
            // }

            String[] nodes_connected_to_a = adjacencyProp.get(node_a).toString().split(" ");

            for (String node : nodes_connected_to_a) {

                // System.out.println("not_connected: " + node + " " + node_b);
                if (node.equals(node_b)) {
                    // System.out.println("connected: " + node + " " + node_b);
                    context.write(key, key);

                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

        }

    }

    public static class CountMapper extends Mapper<Object, Text, Text, NullWritable> {

        // @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Text edge = value;
            context.write(edge, NullWritable.get());

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
        job.setJarByClass(ClusterCoefficient.class);
        job.setMapperClass(ClusterCoefficient.Map.class);
        job.setReducerClass(ClusterCoefficient.Reduce.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        // Path distributed_cache_path = new Path(args[1] + "/part-r-00000");

        Configuration LengthTwoconf = new Configuration();
        Job LengthTwojob = Job.getInstance(LengthTwoconf, "distinct nodes new");
        // LengthTwojob.addCacheFile(distributed_cache_path.toUri());
        LengthTwojob.setJarByClass(ClusterCoefficient.class);
        LengthTwojob.setMapperClass(ClusterCoefficient.LengthTwoMap.class);
        LengthTwojob.setReducerClass(ClusterCoefficient.LengthTwoReduce.class);
        LengthTwojob.setNumReduceTasks(1);
        LengthTwojob.setMapOutputKeyClass(Text.class);
        LengthTwojob.setMapOutputValueClass(Text.class);
        LengthTwojob.setOutputKeyClass(Text.class);
        LengthTwojob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(LengthTwojob, new Path(args[1]));
        FileOutputFormat.setOutputPath(LengthTwojob, new Path(args[2]));

        LengthTwojob.waitForCompletion(true);

        Path distributed_cache_path = new Path(args[1] + "/part-r-00000");

        Configuration IdentifyTrianglesConf = new Configuration();
        Job IdentifyTrianglesJob = Job.getInstance(IdentifyTrianglesConf, "distinct nodes new");
        IdentifyTrianglesJob.addCacheFile(distributed_cache_path.toUri());
        IdentifyTrianglesJob.setJarByClass(ClusterCoefficient.class);
        IdentifyTrianglesJob.setMapperClass(ClusterCoefficient.IdentifyTrianglesMap.class);
        IdentifyTrianglesJob.setReducerClass(ClusterCoefficient.IdentifyTrianglesReduce.class);
        IdentifyTrianglesJob.setNumReduceTasks(1);
        IdentifyTrianglesJob.setMapOutputKeyClass(Text.class);
        IdentifyTrianglesJob.setMapOutputValueClass(Text.class);
        IdentifyTrianglesJob.setOutputKeyClass(Text.class);
        IdentifyTrianglesJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(IdentifyTrianglesJob, new Path(args[2]));
        FileOutputFormat.setOutputPath(IdentifyTrianglesJob, new Path(args[3]));

        IdentifyTrianglesJob.waitForCompletion(true);

        // counting connected triples

        Configuration ConnectedTriplesConf = new Configuration();
        Job ConnectedTriplesJob = Job.getInstance(ConnectedTriplesConf, "distinct nodes new");
        // LengthTwojob.addCacheFile(distributed_cache_path.toUri());
        ConnectedTriplesJob.setJarByClass(ClusterCoefficient.class);
        ConnectedTriplesJob.setMapperClass(ClusterCoefficient.CountMapper.class);
        ConnectedTriplesJob.setReducerClass(ClusterCoefficient.CountReducer.class);
        ConnectedTriplesJob.setNumReduceTasks(1);
        ConnectedTriplesJob.setMapOutputKeyClass(Text.class);
        ConnectedTriplesJob.setMapOutputValueClass(NullWritable.class);
        ConnectedTriplesJob.setOutputKeyClass(IntWritable.class);
        ConnectedTriplesJob.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(ConnectedTriplesJob, new Path(args[2]));
        FileOutputFormat.setOutputPath(ConnectedTriplesJob, new Path(args[4]));

        ConnectedTriplesJob.waitForCompletion(true);

        // counting triangles

        Configuration CountTrianglesConf = new Configuration();
        Job CountTrianglesJob = Job.getInstance(CountTrianglesConf, "distinct nodes new");
        // LengthTwojob.addCacheFile(distributed_cache_path.toUri());
        CountTrianglesJob.setJarByClass(ClusterCoefficient.class);
        CountTrianglesJob.setMapperClass(ClusterCoefficient.CountMapper.class);
        CountTrianglesJob.setReducerClass(ClusterCoefficient.CountReducer.class);
        CountTrianglesJob.setNumReduceTasks(1);
        CountTrianglesJob.setMapOutputKeyClass(Text.class);
        CountTrianglesJob.setMapOutputValueClass(NullWritable.class);
        CountTrianglesJob.setOutputKeyClass(IntWritable.class);
        CountTrianglesJob.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(CountTrianglesJob, new Path(args[3]));
        FileOutputFormat.setOutputPath(CountTrianglesJob, new Path(args[5]));

        CountTrianglesJob.waitForCompletion(true);

        System.exit(CountTrianglesJob.waitForCompletion(true) ? 0 : 1);
    }
}
