package task.mapreduce;

import java.util.Iterator;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



/**
 * MAP-REDUCE
 * 
 * @author zzjzhang
 */
public class Task1 {

	public static class Mapper1 extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
		private IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, OutputCollector<Text, IntWritable> outPut, Reporter reporter) throws IOException {
			String valueStr = value.toString();
			word.set(valueStr);
			outPut.collect(word, one);
		}
	}

	public static class Reducer1 extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable sum = new IntWritable();

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> outPut, Reporter reporter) throws IOException {
			int volume = 0;

			while(values.hasNext()) {
				volume = volume + values.next().get();
			}

			sum.set(volume);
			outPut.collect(key, sum);
		}
	}

	public static void main(String[] args) throws IOException {

		JobConf jobConf = new JobConf();
		jobConf.setJobName("Task1");

		jobConf.setMapOutputKeyClass(Text.class);
		jobConf.setMapOutputValueClass(IntWritable.class);

		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(IntWritable.class);

		jobConf.setMapperClass(Mapper1.class);
		jobConf.setReducerClass(Reducer1.class);

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);

		// 设置 Reduce 任务 数量
		jobConf.setNumReduceTasks(1);

		FileInputFormat.addInputPath(jobConf, new Path(""));
		FileOutputFormat.setOutputPath(jobConf, new Path(""));

		JobClient.runJob(jobConf);
		System.exit(0);
	}

}