package br.ufc.great.hadoop.reviews.eiffel;

import java.io.File;
import java.io.IOException;

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

import br.ufc.great.hadoop.commons.model.Review;
import br.ufc.great.hadoop.commons.utils.MyEnv;
import br.ufc.great.hadoop.commons.utils.MyFileUtils;
import br.ufc.great.hadoop.commons.utils.ReadJson;

public class TemporalDistribution {
	
	public static class MapperClass extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object key, Text data, Context context) throws IOException, InterruptedException {
			Review review = ReadJson.getReview(data.toString());
			context.write(new Text(review.getFormattedDate()), new IntWritable(1));
		}
	}
	
	public static class ReducerClass extends Reducer<Text, IntWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			for (IntWritable val : values) {
				count += val.get();
			}
			context.write(new Text(key + "\t" + count), null);
		}
	}

	public static void main(String[] args) throws Exception {
		String input_dir = null, output_dir = null;
		if(MyEnv.isDevelopment) {
			// Only in development environment...
			input_dir = "/home/paulodias99/Desktop/dataset";
			output_dir = "/home/paulodias99/Desktop/dataset/results";
			
			File output_dirFile = new File(output_dir);
			if(output_dirFile.exists()) {
				MyFileUtils.delete(output_dirFile);
			}
		}else {
			input_dir = args[0];
			output_dir = args[1];
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "q3d-temporal-distribution");
		job.setJarByClass(TemporalDistribution.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input_dir));
		FileOutputFormat.setOutputPath(job, new Path(output_dir));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}