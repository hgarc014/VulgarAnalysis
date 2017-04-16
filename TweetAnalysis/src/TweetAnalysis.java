import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class TweetAnalysis extends Configured implements Tool {
	public static int caTotal = 0;

	// public static class TokenizerMapper extends Mapper<Object, Text, Text,
	// IntWritable> {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, TweetObject> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		HashMap<String, Float> badTweets = new HashMap<String, Float>();
		HashMap<String, Float> goodTweets = new HashMap<String, Float>();
		// JSONObject tweet = new JSONObject();
		JSONObject updated = new JSONObject();
		JSONParser parser = new JSONParser();

		// Object obj = parser.parse(value);

		public void map(Object key, Text value, Context context) throws IOException,
				InterruptedException {

			try {

				JSONObject tweet = (JSONObject) parser.parse(value.toString());

				String lat = tweet.get("lat").toString();
				String lon = tweet.get("long").toString();
				String text = tweet.get("text").toString();
				String state = tweet.get("state").toString();
				String username = tweet.get("username").toString();
				String id = tweet.get("id").toString();

				Integer score;
				try {
					score = (int) (100 * Double.valueOf(tweet.get("score").toString()));
				} catch (NumberFormatException e) {
					score = -1;
				}
				// String state = tweet.get("countryCode").toString();

				if (state == null) {
					System.err.println("state NULL");
				} else if (state.length() > 3) {

					// fullName = fullName.substring(fullName.length() - 3,
					// fullName.length() - 1)
					// .toUpperCase();
				}

				TweetObject t = new TweetObject(score, id, lat, lon, text, username);

				t.setTotal(1);
				t.setBadTotal(1);
				word.set(state);

				context.write(word, t);

				// StringTokenizer itr = new StringTokenizer(text.toString());
				// while (itr.hasMoreTokens()) {
				//
				// // word.set(itr.nextToken());
				// // context.write(word, one);
				// }

			} catch (ParseException e) {
				e.printStackTrace();
			}

			// StringTokenizer itr = new StringTokenizer(value.toString());
			// while (itr.hasMoreTokens()) {
			// word.set(itr.nextToken());
			// context.write(word, one);
			// }
		}
	}

	// public static class IntSumReducer extends Reducer<Text, IntWritable,
	// Text, IntWritable> {
	public static class IntSumReducer extends Reducer<Text, TweetObject, Text, TweetObject> {
		// private IntWritable result = new IntWritable();
		private TweetObject result = new TweetObject();

		// int total = 0;

		// public void reduce(Text key, Iterable<IntWritable> values, Context
		// context)
		public void reduce(Text key, Iterable<TweetObject> values, Context context)
				throws IOException, InterruptedException {

			int score = 0;
			int total = 0;
			int badTotal = 0;
			// Text myt = new Text();

			// myt.set("CA");
			// for (IntWritable val : values) {
			for (TweetObject val : values) {

				score += val.getScore();

				total += val.getTotal();
				if (val.getScore() > 0)
					badTotal += val.getBadTotal();

				// result.updateTotal(val.getTotal());
				// result.updateBadTotal(val.getBadTotal());

				// result.updateTotal(val.getTotal());
				// if (val.getScore() > 0)
				// result.updateBadTotal(val.getBadTotal());
			}

			result.setTotal(total);
			result.setBadTotal(badTotal);
			result.setScore(score);

			context.write(key, result);

		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TweetAnalysis(), args);
		// int res = ToolRunner.run(new TweetAnalysis(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("Usage: runProgram <input dir> <output directory>");
			System.exit(0);
		}

		// When implementing tool
		Configuration conf = this.getConf();

		Job job = Job.getInstance(conf, "Analyse Tweets");
		job.setJarByClass(TweetAnalysis.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TweetObject.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// System.exit(job.waitForCompletion(true) ? 0 : 1);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
