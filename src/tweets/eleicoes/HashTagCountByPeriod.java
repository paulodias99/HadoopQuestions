package br.ufc.great.hadoop.tweets.eleicoes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import br.ufc.great.hadoop.commons.model.Tweet;
import br.ufc.great.hadoop.commons.utils.MyEnv;
import br.ufc.great.hadoop.commons.utils.MyFileUtils;
import br.ufc.great.hadoop.commons.utils.ReadTSV;

public class HashTagCountByPeriod {
	
	public static class MapperClass extends Mapper<Object, Text, Text, MapWritable> {
		public void map(Object key, Text data, Context context) throws IOException, InterruptedException {
			Tweet tweet = ReadTSV.parse(data.toString());
			ArrayList<String> tags = tweet.getHashTags();
			for (String tag : tags) {
				MapWritable map = new MapWritable();
				map.put(new Text("morning"), (tweet.isMorning()) ? new IntWritable(1) : new IntWritable(0));
				map.put(new Text("afternoon"), (tweet.isAfternoon()) ? new IntWritable(1) : new IntWritable(0));
				map.put(new Text("night"), (tweet.isNight()) ? new IntWritable(1) : new IntWritable(0));
				context.write(new Text(tag), map);
			}
		}
	}
	
	public static class ReducerClass extends Reducer<Text, MapWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			int morning = 0, afternoon = 0, night = 0;
			for (MapWritable val : values) {
				morning   += ((IntWritable) val.get(new Text("morning"))).get();
				afternoon += ((IntWritable) val.get(new Text("afternoon"))).get();
				night 	  += ((IntWritable) val.get(new Text("night"))).get();
			}
			context.write(new Text(key + "\t" + morning + "\t" + afternoon + "\t" + night), null);
		}
	}

	public static void main(String[] args) throws Exception {
		String input_dir = null, output_dir = null;
		if(MyEnv.isDevelopment) {
			// Only in development environment...
			input_dir = "D:/dev/github/pedroalmir/cloud_study/hadoop/tests/tweets";
			output_dir = "D:/dev/github/pedroalmir/cloud_study/hadoop/tests/output";
			
			File output_dirFile = new File(output_dir);
			if(output_dirFile.exists()) {
				MyFileUtils.delete(output_dirFile);
			}
		}else {
			input_dir = args[0];
			output_dir = args[1];
		}
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "q2a-hashtag-count-by-period");
		job.setJarByClass(HashTagCountByPeriod.class);
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(input_dir));
		FileOutputFormat.setOutputPath(job, new Path(output_dir));

		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}