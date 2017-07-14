package WordCount.WordCount;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
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

public class WordCount_positive_negative {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void setup(Context c) throws IOException, InterruptedException {
			getpositivelist();
			getnegativelist();
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String words[] = value.toString().replaceAll("[^a-zA-Z-+ ]", " ")
					.toLowerCase().split("\\s+");

			for (int i = 0; i < words.length; i++) {
				String current = words[i];
				if (positive.contains(current)) {
					word.set("positive");
					context.write(word, one);
				}
				if (negative.contains(current)) {
					word.set("negative");
					context.write(word, one);
				}
			}
		}

		public void getpositivelist() throws IOException {

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					this.getClass().getResourceAsStream("/positive-words.txt")));
			String line;
			while ((line = reader.readLine()) != null) {
				positive.add(line);
			}
			reader.close();
		}

		public void getnegativelist() throws IOException {

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					this.getClass().getResourceAsStream("/negative-words.txt")));
			String line;
			while ((line = reader.readLine()) != null) {
				negative.add(line);
			}
			reader.close();
		}

		public List<String> positive = new ArrayList<String>();
		public List<String> negative = new ArrayList<String>();
	}

	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
		conf.set("mapreduce.framework.name", "yarn");
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}