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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class GeodesicPath {

    public static class AdjacencyMap extends Mapper<Object, Text, Text, Text> {
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

    public static class AdjacencyReduce extends Reducer<Text, Text, Text, NullWritable> {

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

    public static class LengthOneMap extends Mapper<Object, Text, Text, Text> {
        // private Text node;

        // @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // node = value;

            String line = value.toString();

            String[] split_line = line.split(" ");

            String first_node = split_line[0];
            String second_node = split_line[1];

            if (first_node.compareTo(second_node) > 0) {
                String outkey = second_node + " " + first_node;
                String outval = second_node + "~" + first_node;

                context.write(new Text(outkey), new Text(outval));
            } else {
                String outkey = first_node + " " + second_node;
                String outval = first_node + "~" + second_node;

                context.write(new Text(outkey), new Text(outval));
            }

            // context.write(new Text(first_node), new Text(second_node));
            // context.write(new Text(second_node), new Text(first_node));

        }
    }

    public static class LengthOneReduce extends Reducer<Text, Text, Text, NullWritable> {

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

        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

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

    // public static class LengthThreeMap extends Mapper<Object, Text, Text,
    // NullWritable> {
    // // private Text node;

    // // @Override
    // protected void map(Object key, Text value, Context context) throws
    // IOException, InterruptedException {

    // // String line = value.toString();

    // // String[] split_line = line.split("\t");

    // context.write(value, NullWritable.get());

    // }
    // }

    // public static class LengthThreeReduce extends Reducer<Text, NullWritable,
    // Text, Text> {

    // private Properties adjacencyProp = new Properties();

    // @Override
    // protected void setup(Context context) throws IOException,
    // InterruptedException {

    // InputStream iStream = new FileInputStream("./part-r-00000");

    // adjacencyProp.load(iStream);

    // }

    // protected void reduce(Text key, Iterable<Text> values, Context context)
    // throws IOException, InterruptedException {

    // String[] key_split = key.toString().split("\t");

    // String[] nodes = key_split[0].split(" ");

    // String node_a = nodes[0];
    // String node_b = nodes[1];

    // String[] path = key_split[1].split("~");

    // String middle_node = path[1];

    // String[] nodes_connected_to_a = adjacencyProp.get(node_a).toString().split("
    // ");
    // String[] nodes_connected_to_b = adjacencyProp.get(node_a).toString().split("
    // ");

    // if (nodes_connected_to_a.length > 1) {

    // for (String node : nodes_connected_to_a) {
    // if (!node.equals(middle_node)) {

    // }
    // }

    // }

    // }

    // }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "distinct nodes new");
        job.setJarByClass(GeodesicPath.class);
        job.setMapperClass(GeodesicPath.AdjacencyMap.class);
        job.setReducerClass(GeodesicPath.AdjacencyReduce.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        Configuration LengthOneConf = new Configuration();
        Job LengthOneJob = Job.getInstance(LengthOneConf, "distinct nodes new");
        LengthOneJob.setJarByClass(GeodesicPath.class);
        LengthOneJob.setMapperClass(GeodesicPath.LengthOneMap.class);
        LengthOneJob.setReducerClass(GeodesicPath.LengthOneReduce.class);
        LengthOneJob.setNumReduceTasks(1);
        LengthOneJob.setMapOutputKeyClass(Text.class);
        LengthOneJob.setMapOutputValueClass(Text.class);
        LengthOneJob.setOutputKeyClass(Text.class);
        LengthOneJob.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(LengthOneJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(LengthOneJob, new Path(args[2]));

        LengthOneJob.waitForCompletion(true);

        Configuration LengthTwoconf = new Configuration();
        Job LengthTwojob = Job.getInstance(LengthTwoconf, "distinct nodes new");
        LengthTwojob.setJarByClass(GeodesicPath.class);
        LengthTwojob.setMapperClass(GeodesicPath.LengthTwoMap.class);
        LengthTwojob.setReducerClass(GeodesicPath.LengthTwoReduce.class);
        LengthTwojob.setNumReduceTasks(1);
        LengthTwojob.setMapOutputKeyClass(Text.class);
        LengthTwojob.setMapOutputValueClass(Text.class);
        LengthTwojob.setOutputKeyClass(Text.class);
        LengthTwojob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(LengthTwojob, new Path(args[1]));
        FileOutputFormat.setOutputPath(LengthTwojob, new Path(args[3]));

        LengthTwojob.waitForCompletion(true);

        // java.nio.file.Path distributed_cache_path = new Path(args[1] +
        // "/part-r-00000");

        // Configuration LengthThreeconf = new Configuration();
        // Job LengthThreejob = Job.getInstance(LengthThreeconf, "distinct nodes new");
        // LengthThreejob.addCacheFile(distributed_cache_path.toUri());
        // LengthThreejob.setJarByClass(GeodesicPath.class);
        // LengthThreejob.setMapperClass(GeodesicPath.LengthTwoMap.class);
        // LengthThreejob.setReducerClass(GeodesicPath.LengthTwoReduce.class);
        // LengthThreejob.setNumReduceTasks(1);
        // LengthThreejob.setMapOutputKeyClass(Text.class);
        // LengthThreejob.setMapOutputValueClass(NullWritable.class);
        // LengthThreejob.setOutputKeyClass(Text.class);
        // LengthThreejob.setOutputValueClass(Text.class);
        // FileInputFormat.addInputPath(LengthThreejob, new Path(args[3]));
        // FileOutputFormat.setOutputPath(LengthThreejob, new Path(args[4]));

        // LengthThreejob.waitForCompletion(true);

        System.exit(LengthTwojob.waitForCompletion(true) ? 0 : 1);
    }
}
