package com.lc.mr.pagerank;

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

import java.io.IOException;

public class RunJob {

	public static enum  Mycounter{
		my
	}

	public static void main(String[] args) {
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://linux1:8020");
		config.set("yarn.resourcemanager.hostname", "linux1");
		//config.set("mapred.jar", "D:\\testProduct\\wc.jar");
		double differenceIndex = 0.001;//定义差值指标  需放大一千倍
		int i = 0;
		while(true) {
			i++;

			try {
				FileSystem fs = FileSystem.get(config);

				config.setInt("runCount", i);//添加默认值
				Job job = Job.getInstance(config);
				job.setJarByClass(RunJob.class);

				job.setJobName("pr"+i);

				job.setMapperClass(PageRankMapper.class);
				job.setReducerClass(PageRankReducer.class);

				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);

				job.setInputFormatClass(KeyValueTextInputFormat.class);
				Path inputPath = new Path("/test/user/input/pagerank/pagerank.txt");
				if(i > 1) {
					inputPath = new Path("/test/user/output/pagerank/pr"+(i-1));
				}
				FileInputFormat.addInputPath(job,inputPath);

				Path outpath = new Path("/test/user/output/pagerank/pr"+i);
				if (fs.exists(outpath)) {

					fs.delete(outpath, true);
				}

				FileOutputFormat.setOutputPath(job, outpath);

				boolean f = job.waitForCompletion(true);
				if (f) {
					System.out.println("success ");
					//平均差值  所有节点的差值的平均值 就是Reduce的个数，下面设置了4，所以这里也是4
					//用mapReduce 计数器
					long sum = job.getCounters().findCounter(Mycounter.my).getValue();//所有节点差值放大一千倍的和
					double avgd = sum /4000.0;
					System.out.println(sum);
					if(avgd < differenceIndex){//判断是否收敛
						break;
					}

				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	static class PageRankMapper extends Mapper<Text,Text,Text,Text>{
		@Override
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			int runCount = context.getConfiguration().getInt("runCount",1);
			String page = key.toString();
			Node node ;
			if(runCount == 1){
				node = Node.fromMR("1.0"+"\t"+value.toString());
			}else{
				node = Node.fromMR(value.toString());
			}
			context.write(new Text(page),new Text(node.toString()));//key A : value 1.0	B	D
			if(node.containsAdjacentNodes()){
				double  outValue = node.getPageRank()/node.getAdjacentNodeNames().length;
				for(int i = 0 ; i < node.getAdjacentNodeNames().length ; i++){
					String  outPage = node.getAdjacentNodeNames()[i];

					context.write(new Text(outPage),new Text(outValue+""));//key B:0.5  D:0.5
				}
			}



		}
	}

	static class PageRankReducer extends Reducer<Text,Text,Text,Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double sum = 0.0;
			Node sourceNode = null ;
			for(Text i : values){
				Node node = Node.fromMR(i.toString());
				if(node.containsAdjacentNodes()){
					sourceNode = node;//获取未计算之前的内容，原pageRank
				}else{
					sum  = sum + node.getPageRank();
				}
			}
			//mapReduce个数就不再设置了，可仿照weather
			//这里写死

			//这里把pageRank的公式补充完整
			double q = 0.85;

			double newPR = ((1-q)/4) + (q*sum) ;
			System.out.println("******  new PageRank value is "+ newPR);
			//将新的PR和原来的PR值比较  用于平均差值收敛用
			//if(ne)


			double pageRankDifference = newPR - sourceNode.getPageRank(); //大一点或小一点  此时应求绝对值
			//MapReduce 计数器

			int PRIndex = (int) (pageRankDifference *1000.0); //直接取整数部分

			PRIndex = Math.abs(PRIndex);
			System.out.println("_________"+PRIndex);
			//由于传入为long 类型，所以要把差值放大，具体放大多少根据业务需求（差值指标）决定，这里为减少计算次数放大一千倍
			context.getCounter(Mycounter.my).increment(PRIndex);

			sourceNode.setPageRank(newPR);
			context.write(key,new Text(sourceNode.toString()));

		}
	}
}
