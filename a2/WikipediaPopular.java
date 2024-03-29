// adapted from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class WikipediaPopular extends Configured implements Tool {

	public static class TokenizerMapper
	extends Mapper<LongWritable, Text, Text, LongWritable>{

		private final static LongWritable one = new LongWritable(1L);
		private Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context
				) throws IOException, InterruptedException {
					StringTokenizer rawString = new StringTokenizer(value.toString());

					String date = rawString.nextToken();
					String language = rawString.nextToken();
					String title = rawString.nextToken();
					String times = rawString.nextToken();

					if ((language.equals("en")) && !(title.equals("Main_Page")) && !(title.startsWith("Special:"))){
						word.set(date);
						one.set(Long.valueOf(times));
						context.write(word, one);
					}
			
			}	
		}
	

	public static class LongMaxReducer
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		@Override
		public void reduce(Text date, Iterable<LongWritable> values,
				Context context
				) throws IOException, InterruptedException {
			long maxCount = 0L;
			for (LongWritable val : values) {
				maxCount = val.get() > maxCount ? val.get() : maxCount;
			}
			result.set(maxCount);
			context.write(date, result);
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		Job job = Job.getInstance(conf, "Wikipedia Popular");
		job.setJarByClass(WikipediaPopular.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(LongMaxReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextInputFormat.addInputPath(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
