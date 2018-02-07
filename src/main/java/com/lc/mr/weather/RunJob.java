package com.lc.mr.weather;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import sun.java2d.pipe.SpanShapeRenderer.Simple;

public class RunJob {

	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://linux1:8020");
		 config.set("yarn.resourcemanager.hostname", "linux1");
		//config.set("mapred.jar", "D:\\testProduct\\wc.jar");
		try {
			FileSystem fs = FileSystem.get(config);

			Job job = Job.getInstance(config);
			job.setJarByClass(RunJob.class);

			job.setJobName("weather");
			
			job.setMapperClass(WeatherMapper.class);
			
			job.setReducerClass(WeatherReducer.class);
			
			job.setMapOutputKeyClass(MyKey.class);
			
			job.setMapOutputValueClass(DoubleWritable.class);
			
			job.setPartitionerClass(MyPartitioner.class);
			
			job.setSortComparatorClass(MySort.class);
			
			job.setGroupingComparatorClass(MyGroup.class);
			
			job.setNumReduceTasks(3);
			
			job.setInputFormatClass(KeyValueTextInputFormat.class);//添加默认分割符


			FileInputFormat.addInputPath(job, new Path("/test/user/input/weather"));

			Path outpath = new Path("/test/user/output/weather");
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("job完成");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//key  是每一行第一个隔开符左侧为key右侧围value
	static class WeatherMapper extends Mapper<Text, Text, MyKey, DoubleWritable>{
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		protected void map(Text key, Text value, Mapper<Text, Text, MyKey, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println(key.toString());
			try {
				Date date = sdf.parse(key.toString());
				Calendar c = Calendar.getInstance();
				c.setTime(date);
				int year = c.get(Calendar.YEAR);
				int month = c.get(Calendar.MONTH);
				String hotStr = value.toString();
				double hot =Double.parseDouble(hotStr.substring(0, hotStr.lastIndexOf("c")));
				
				MyKey k = new MyKey();
				k.setYear(year);
				k.setMonth(month);
				k.setHot(hot);
				context.write(k, new DoubleWritable(hot));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	static class WeatherReducer extends Reducer<MyKey, DoubleWritable, Text, NullWritable>{

		protected void reduce(MyKey arg0, Iterable<DoubleWritable> arg1,
				Context arg2)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
		int i = 0;
		for(DoubleWritable v : arg1){
			i++;
			String msg =  arg0.getYear()+"\t"+(arg0.getMonth()+1)+"\t"+v.get();
			arg2.write(new Text(msg),NullWritable.get());
			if(i == 3){
				break;
			}
		}
		}
		
	}
}
