package WordCount.WordCount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.Hash;

public class WordCount_partsofspeech {

	public static class TokenizerMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void setup(Context c) throws IOException, InterruptedException {
			readPOS();
		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {

			String words[] = value.toString().replaceAll("[^a-zA-Z-+ ]", " ")
					.split("\\s+");

			for (int i = 0; i < words.length; i++) {
				if (words[i] != null) {
					if (words[i].length() < 5) {
						continue;
					} else {
						if (hash.get(words[i]) != null) {
							String type = hash.get(words[i]);
							char temp[] = words[i].toCharArray();
							if (type != null) {
								if (isPalindrome(temp) == true) {
									type = type + "##Palindromes";
								}
								word.set(type);
								context.write(
										new IntWritable(words[i].length()),
										word);
							}
						}
					}
				}
			}
		}

		private HashMap<String, String> hash;

		public boolean isPalindrome(char word[]) {
			int i = 0, j = word.length - 1;
			while (i < j) {
				if (word[i] != word[j])
					return false;
				i++;
				j--;
			}
			return true;
		}

		public void readPOS() throws IOException {

			hash = new HashMap<String, String>();

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					this.getClass().getResourceAsStream("/mobyposi.txt"),
					"Cp1252"));
			String line;

			while ((line = reader.readLine()) != null) {
				String words[] = new String[2];// =
				line = line.replaceAll("[^a-zA-Z0-9-+'/ ]", "#");
				words[0] = line.substring(0, line.indexOf("#"));
				// Considering only the first type of part of speech if there
				// are multiple associated with a word
				words[1] = line.substring(line.indexOf("#") + 1,
						line.indexOf("#") + 2);
				if (words[1] != null) {
					String type = words[1].substring(0, 1);
					if (type.equals("N"))
						hash.put(words[0], "Noun");
					else if (type.equals("p"))
						hash.put(words[0], "Plural");
					else if (type.equals("h"))
						hash.put(words[0], "Noun Phrase");
					else if (type.equals("V") || type.equals("t")
							|| type.equals("i"))
						hash.put(words[0], "Verb");
					else if (type.equals("A"))
						hash.put(words[0], "Adjective");
					else if (type.equals("v"))
						hash.put(words[0], "Adverb");
					else if (type.equals("C"))
						hash.put(words[0], "Conjunction");
					else if (type.equals("P"))
						hash.put(words[0], "Preposition");
					else if (type.equals("!"))
						hash.put(words[0], "Interjection");
					else if (type.equals("r"))
						hash.put(words[0], "Pronoun");
					else if (type.equals("D") || type.equals("I"))
						hash.put(words[0], "Article");
					else if (type.equals("o"))
						hash.put(words[0], "Nominative");
				}
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<IntWritable, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			HashMap<String, Integer> h = new HashMap<String, Integer>();
			int count;
			int counter = 0;
			int palcount = 0;

			String sum = "";
			for (Text val : values) {
				sum += val.toString() + "##";
				counter++;
			}

			String group[] = sum.split("##");
			for (String g : group) {
				if (g.equals("Palindromes")) {
					palcount++;
				} else {
					if (!h.containsKey(g))
						h.put(g, 1);
					else {
						count = h.get(g) + 1;
						h.put(g, count);
					}
				}
			}

			result.set("\nCount of Words: " + counter
					+ "\nDistribution of POS: " + h.toString()
					+ "\nNumber of Palindromes: " + palcount);
			context.write(new Text("\nLength: " + key.toString()), result);
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
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
