import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Kmeans {

  public static double parseDouble(Writable writable) {
    return Double.parseDouble(writable.toString());
  } 

  public static class DoubleArrayWritable extends ArrayWritable {

    public DoubleArrayWritable() {
      super(DoubleWritable.class);
    }

    public DoubleArrayWritable(String[] strings) {

      super(DoubleWritable.class);
      Writable[] values = new Writable[strings.length];

      for (int i = 0; i < values.length; ++i) {
        values[i] = new DoubleWritable(Double.valueOf(strings[i]));
      }

      set(values);

    }

    public DoubleArrayWritable(double[] doubles) {

      super(DoubleWritable.class);
      Writable[] values = new Writable[doubles.length];

      for (int i = 0; i < values.length; ++i) {
        values[i] = new DoubleWritable(Double.valueOf(doubles[i]));
      }

      set(values);

    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (String s : super.toStrings()) {
        sb.append(s).append(" ");
      }
      return sb.toString();
    }

  }

  public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, DoubleArrayWritable>{

    List<DoubleArrayWritable> centroids = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException {

      Path pt = new Path("centroids");
      FileSystem fs = FileSystem.get(new Configuration());
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));

      String line;

      while ((line = br.readLine()) != null) {
        centroids.add(new DoubleArrayWritable(line.split(" ")));
      }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      DoubleArrayWritable point = new DoubleArrayWritable(value.toString().split(" "));

      context.write(new Text(closestCentroid(point).toString()), point);

    }

    public DoubleArrayWritable closestCentroid(DoubleArrayWritable point) {

      double minDistance = Double.MAX_VALUE;
      DoubleArrayWritable closestCentroid = null;

      Writable[] pointValues = point.get();

      for (DoubleArrayWritable centroid : centroids) {

        Writable[] centroidValues = centroid.get();             

        double squareSum = 0;
        for (int i = 0; i < centroidValues.length; ++i) {
          double centroidValue = parseDouble(centroidValues[i]);
          double pointValue = parseDouble(pointValues[i]);
          double difference = centroidValue - pointValue;
          squareSum += difference * difference;
        }

        double distance = Math.sqrt(squareSum);

        if (distance < minDistance) {
          minDistance = distance;
          closestCentroid = centroid;
        }

      }

      return closestCentroid;
    }
  }

  public static class IntSumReducer extends Reducer<Text, DoubleArrayWritable, Text, NullWritable> {

    public void reduce(Text key, Iterable<DoubleArrayWritable> values, Context context)
        throws IOException, InterruptedException {

      int n = 0;

      DoubleArrayWritable centroid = new DoubleArrayWritable(key.toString().split(" "));
      double mean[] = new double[centroid.get().length];

      for (DoubleArrayWritable point : values) {

        Writable[] pointValues = point.get();

        for (int i = 0; i < mean.length; ++i) {
          double pointValue = parseDouble(pointValues[i]);
          mean[i] += pointValue;
        }
        ++n;
      }

      for (int i = 0; i < mean.length; ++i) {
        mean[i] /= n;
      }

      context.write(new Text(new DoubleArrayWritable(mean).toString()), NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Kmeans");

    job.setJarByClass(Kmeans.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleArrayWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}
