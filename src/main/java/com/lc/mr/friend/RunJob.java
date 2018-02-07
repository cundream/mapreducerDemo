package com.lc.mr.friend;

import com.lc.mr.wc.WordCountMapper;
import com.lc.mr.wc.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;

public class RunJob {

	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://linux1:8020");
		config.set("yarn.resourcemanager.hostname", "linux1");
		//config.set("mapred.jar", "D:\\testProduct\\wc.jar");

        if(run1(config)){
            run2(config);
        }

	}

    public static void run2(Configuration config) {
        try {
            FileSystem fs =FileSystem.get(config);
            Job job =Job.getInstance(config);
            job.setJarByClass(RunJob.class);

            job.setJobName("fof2");

            job.setMapperClass(SortMapper.class);
            job.setReducerClass(SortReducer.class);
            job.setSortComparatorClass(FoFSort.class);
            job.setGroupingComparatorClass(FoFGroup.class);
            job.setMapOutputKeyClass(User.class);
            job.setMapOutputValueClass(User.class);

            job.setInputFormatClass(KeyValueTextInputFormat.class);

            //设置MR执行的输入文件
            FileInputFormat.addInputPath(job, new Path("/test/user/output/fof1"));

            //该目录表示MR执行之后的结果数据所在目录，必须不能存在
            Path outputPath=new Path("/test/user/output/fof2");
            if(fs.exists(outputPath)){
                fs.delete(outputPath, true);
            }
            FileOutputFormat.setOutputPath(job, outputPath);

            boolean f =job.waitForCompletion(true);
            if(f){
                System.out.println("job 成功执行");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	public static boolean run1(Configuration config){
        try {
            FileSystem fs = FileSystem.get(config);

            Job job = Job.getInstance(config);
            job.setJarByClass(RunJob.class);

            job.setJobName("friends");

            job.setMapperClass(FofMapper.class);
            job.setReducerClass(FofReducer.class);

            job.setMapOutputKeyClass(Fof.class);
            job.setMapOutputValueClass(IntWritable.class);


            job.setInputFormatClass(KeyValueTextInputFormat.class);//添加默认分割符 \t

            FileInputFormat.addInputPath(job, new Path("/test/user/input/fof"));

            Path outpath = new Path("/test/user/output/fof1");
            if (fs.exists(outpath)) {
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job, outpath);

            boolean f = job.waitForCompletion(true);
            if (f) {
                System.out.println("job完成");
            }

            return f;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

	static class FofMapper extends Mapper<Text,Text,Fof,IntWritable>{
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String user = key.toString();
			String[] friends = StringUtils.split(value.toString(),'\t');
			int length = friends.length;
			for(int i = 0; i < length ; i++){
				String f1 = friends[i];
				Fof ofof = new Fof(user,f1);
				context.write(ofof,new IntWritable(0));
				for( int j = i + 1 ; j < length ; j++){
					String f2 = friends[j];
					Fof fof = new Fof(f1,f2);
					context.write(fof,new IntWritable(1));
				}
			}
		}
	}

	static class FofReducer extends Reducer<Fof,IntWritable,Fof,IntWritable>{
        @Override
        protected void reduce(Fof key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            boolean f = true;

            for(IntWritable i:values){
                if(i.get() == 0){
                    f = false;
                    break;
                }else{
                    sum = sum + i.get();
                }

            }
            if(f){
                context.write(key,new IntWritable(sum));
            }
        }
    }


    static class SortMapper extends Mapper<Text, Text, User, User>{

        protected void map(Text key, Text value,Context context) throws IOException, InterruptedException {
            String[] args=StringUtils.split(value.toString(),'\t');
            String other=args[0];
            int friendsCount =Integer.parseInt(args[1]);
            System.out.println(key.toString()+"--------------SortMapper------------"+args[0]+"---"+args[1]);

            context.write(new User(key.toString(),friendsCount), new User(other,friendsCount));
            context.write(new User(other,friendsCount), new User(key.toString(),friendsCount));
        }
    }

    static class SortReducer extends Reducer<User, User, Text, Text>{
        protected void reduce(User key, Iterable<User> values,
                              Context context)
                throws IOException, InterruptedException {
            String user =key.getUname();
            StringBuffer sb =new StringBuffer();
            System.out.println("--------------SortReducer------------");
            for(User u: values ){
                sb.append(u.getUname()+":"+u.getFriendsCount());
                sb.append(",");
            }
            context.write(new Text(user), new Text(sb.toString()));
        }
    }

}
