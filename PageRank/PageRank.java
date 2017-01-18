//package org.apache.hadoop.PageRank;

import java.util.ArrayList;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.net.URI;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.Math;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.DoubleWritable.Comparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//TODO:secondary sort in hadoop
//http://blog.csdn.net/huashetianzu/article/details/7837934

public class PageRank 
{
	//sum the graph size
	public static class SumMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] line = value.toString().split("\t");
			context.write(new Text(line[0]), new Text("1"));
			context.write(new Text(line[1]), new Text("1"));
		}
	}

	public static class SumReducer extends Reducer<Text, Text, Text, Text> 
	{
		private int sum;
		public void setup(Context context) throws IOException, InterruptedException 
		{
			//context.write(new Text("setup"), new Text("1"));
			sum = 0;
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			//context.write(key, new Text("1"));
			sum = sum + 1;
		}

		public void cleanup(Context context) throws IOException, InterruptedException 
		{
			context.write(new Text("size"), new Text(String.valueOf(sum)));
		}
	}
	//prepare adjancy list
	public static class PreMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] line = value.toString().split("\t");
			context.write(new Text(line[0]), new Text(line[1]));
			//NOTICE:if not emit it here, nodes without out edge will not present at the first loop and the file line number not right
			//WARN:maybe the split function will cause error, because the right part can be "" now
			context.write(new Text(line[1]), new Text(""));
		}
	}

	public static class PreReducer extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Configuration conf = context.getConfiguration();
			//double factor = Double.parseDouble(conf.get("factor"));
			int size = Integer.parseInt(conf.get("size"));
			String p = String.valueOf(1.0/size);
			String s = p + "," + p + ":";
			int cnt = 0;
			for (Text val : values) 
			{
				if(val.toString().equals(""))
				{
					continue;
				}
				cnt = cnt + 1;
				if(cnt > 1)
				{
					s = s + ",";
				}
				s = s + val.toString();
			}
			context.write(key, new Text(s));
		}

	}

	//the page rank iteration
	public static class PGMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] line = value.toString().split("\t");
			String[] content = line[1].split(":");
			String[] links;
			String linkstr;
			if(content.length > 1)
			{
				links = content[1].split(",");
				linkstr = content[1];
			}
			else
			{
				links = null;
				linkstr = "";
			}

			String newrk = content[0].split(",")[1];
			double rank = Double.parseDouble(newrk);
			String word = line[0];
			context.write(new Text(word), new Text("links:" + linkstr));
			context.write(new Text(word), new Text("oldval:" + newrk));
			if(links != null)
			{
				int num = links.length;
				for(String w:links)
				{
					context.write(new Text(w), new Text("rank:" + String.valueOf(rank/num)));
				}
			}
		}
	}

	public static class PGReducer extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Configuration conf = context.getConfiguration();
			double factor = Double.parseDouble(conf.get("factor"));
			int size = Integer.parseInt(conf.get("size"));
			
			String links = "";
			double sum = 0, oldval = 0;
			for (Text val : values) 
			{
				String[] line = val.toString().split(":");
				if(line[0].compareTo("links") == 0)
				{
					if(line.length > 1)
					{
						links = line[1];
					}
				}
				else if(line[0].compareTo("oldval") == 0)
				{
					oldval = Double.parseDouble(line[1]);
				}
				else   //the rank
				{
					double d = Double.parseDouble(line[1]);
					sum += d;
				}
			}
			//System.out.println("    ");
			//System.out.println("sum is "+sum);
			//System.out.println("    ");
			double newval = (1-factor)*sum + factor/size;
			context.write(key, new Text(String.valueOf(oldval) + "," + String.valueOf(newval) + ":" + links));
		}
	}

	//compute loss
	public static class CheckMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] line = value.toString().split("\t");
			String[] content = line[1].split(":");
			String word = line[0];
			String rank0 = content[0].split(",")[0];
			String rank1 = content[0].split(",")[1];
			context.write(new Text(word), new Text(String.valueOf(rank1)));
		}
	}

	//BETTER: check phase and dist phase can be removed, because we can read/write hdfs file using java API in reduce()

	public static class CheckReducer extends Reducer<Text, Text, Text, Text> 
	{
		private double loss;
		public void setup(Context context) throws IOException, InterruptedException 
		{
			loss = 1.0;
		}

		public void cleanup(Context context) throws IOException, InterruptedException 
		{
			context.write(new Text("loss"), new Text(String.valueOf(loss)));
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			for (Text val : values) 
			{
				loss = loss - Double.parseDouble(val.toString());
			}
		}
	}

	//redistribute the loss probability to ensure teh sum is 1.0
	public static class LossMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] line = value.toString().split("\t");
			String[] content = line[1].split(":");
			String word = line[0];
			String rank0 = content[0].split(",")[0];
			String rank1 = content[0].split(",")[1];
			context.write(new Text(word), new Text("oldval:" + rank0));
			context.write(new Text(word), new Text("newval:" + rank1));
			String linkstr;
			if(content.length > 1)
			{
				linkstr = content[1];
			}
			else
			{
				linkstr = "";
			}
			context.write(new Text(word), new Text("links:" + linkstr));
		}
	}

	public static class LossReducer extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Configuration conf = context.getConfiguration();
			double factor = Double.parseDouble(conf.get("factor"));
			double loss = Double.parseDouble(conf.get("loss"));
			int size = Integer.parseInt(conf.get("size"));
			String oldval = "";
			double newval = 0.0;
			String linkstr = "";

			for (Text val : values) 
			{
				String[] line = val.toString().split(":");
				if(line[0].equals("oldval"))
				{
					oldval = line[1];
				}
				else if(line[0].equals("newval"))
				{
					newval = Double.parseDouble(line[1]);
				}
				else   //links
				{
					if(line.length > 1)
					{
						linkstr = line[1];
					}
				}
			}

			newval = (loss/size + newval) * (1 - factor) + factor * (1.0/size);
			context.write(key, new Text(oldval + "," + String.valueOf(newval) + ":" + linkstr));
		}
	}


	//compute distance
	public static class DistMapper extends Mapper<Object, Text, Text, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] line = value.toString().split("\t");
			String[] content = line[1].split(":");
			String word = line[0];
			String rank0 = content[0].split(",")[0];
			String rank1 = content[0].split(",")[1];
			double dist = Math.abs(Double.parseDouble(rank0) - Double.parseDouble(rank1));
			context.write(new Text(word), new Text(String.valueOf(dist)));
		}
	}

	public static class DistReducer extends Reducer<Text, Text, Text, Text> 
	{
		private double dist;
		//private double dist;
		public void setup(Context context) throws IOException, InterruptedException 
		{
			dist = 0.0;
		}

		public void cleanup(Context context) throws IOException, InterruptedException 
		{
			context.write(new Text("dist"), new Text(String.valueOf(dist)));
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			for (Text val : values) 
			{
				dist = dist + Math.pow(Double.parseDouble(val.toString()), 2);
			}
		}
	}

	//the end sort phase
	public static class myComparator extends Comparator
	{
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b)
		{
			return -super.compare(a, b);
		}
		//QUERY:ho wto sort on value using double descending, while on key by string ascending
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{
			//compare the string: double on - while string on +
			return super.compare(b1, s1, l1, b2, s2, l2);
			//return -super.compare(b1, s1, l1, b2, s2, l2);
		}
	}

	public static class EndMapper extends Mapper<Object, Text, DoubleWritable, Text> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] line = value.toString().split("\t");
			String[] content = line[1].split(":");
			String word = line[0];
			//only save the new value if has two
			String rank = content[0].split(",")[1];
			context.write(new DoubleWritable(Double.parseDouble(rank)), new Text(word));
		}
	}

	public static class EndReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> 
	{
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			for(Text text:values)
			{
				context.write(text, key);
			}
		}
	}

    public static void main(String[] args) throws Exception 
	{
		long startTime=System.currentTimeMillis();
		boolean flag;
		int size = 0;
		int loop_limit = 100;
		double epison = 0.0001;
		String hdfs = "hdfs://172.31.222.90:9000/user/hadoop/";
		String loopFile = "hdfs://172.31.222.90:9000/tmp/loop/";
		String loop2File = "hdfs://172.31.222.90:9000/tmp/loop2/";
		String sumFile = "hdfs://172.31.222.90:9000/tmp/sum/";
		String checkFile = "hdfs://172.31.222.90:9000/tmp/check/";

		Configuration sum_conf = new Configuration();
		sum_conf.set("mapred.jar", "PageRank.jar");
		//NOTICE: use \t as output divider by default
		//conf.set("mapred.textoutputformat.separator", ":");
		String[] otherArgs = new GenericOptionsParser(sum_conf, args).getRemainingArgs();
		if(otherArgs.length < 2)
		{
			System.err.println("Usage: PageRank <input> <output>");
			System.exit(2);
		}

		//the probability of random walk
        double factor = 0;
        if(otherArgs.length > 2)
		{
            factor = Double.parseDouble(otherArgs[2]);
        }
		else
		{
            factor=0.15;
        }

		//FIRST: sum the graph size as global parameter
		Path sum_outputPath = new Path(sumFile);
		sum_outputPath.getFileSystem(sum_conf).delete(sum_outputPath, true);
		Job sum_job = Job.getInstance(sum_conf, "Page Rank");
		//need to set to 1 to ensure correctness
		sum_job.setNumReduceTasks(1);
		sum_job.setJarByClass(PageRank.class);
		sum_job.setMapperClass(SumMapper.class);
		//TODO:this can help, but not use Reducer please
		//sum_job.setCombinerClass(SumReducer.class);
		sum_job.setReducerClass(SumReducer.class);
		sum_job.setOutputKeyClass(Text.class);
		sum_job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(sum_job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(sum_job, sum_outputPath);
		flag = sum_job.waitForCompletion(true);
		//read and count the file lines
		String path = sum_outputPath + "/part-r-00000";
		//StringBuffer buffer = new StringBuffer();
		String lineText;
		FileSystem fs = FileSystem.get(URI.create(path), sum_conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(path));
		BufferedReader bufferRead = new BufferedReader(new InputStreamReader(hdfsInStream));
		while((lineText = bufferRead.readLine()) != null)
		{
			size = Integer.parseInt(lineText.split("\t")[1].trim());
		}
		bufferRead.close();
		//byte[] ioBuffer = new byte[1024];
		//int readLen = hdfsInStream.read(ioBuffer);
		//while(readLen != -1)
		//{
			//readLen = hdfsInStream.read(ioBuffer);
		//}
		hdfsInStream.close();
		fs.close();
		//parse the size from byte[]
		//int offset = 5; //size\t
		//int b0 = ioBuffer[offset] & 0xFF;
		//int b1 = ioBuffer[offset+1] & 0xFF;
		//int b2 = ioBuffer[offset+2] & 0xFF;
		//int b3 = ioBuffer[offset+3] & 0xFF;
		//size = (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
		System.out.println("graph size: " + size);
		
		//SECOND:run the pre map to prepare for pagerank iteration
		Configuration pre_conf = new Configuration();
		pre_conf.set("mapred.jar", "PageRank.jar");
		pre_conf.set("factor", String.valueOf(factor));
		pre_conf.set("size", String.valueOf(size));
		Path pre_outputPath = new Path(loopFile);
		pre_outputPath.getFileSystem(pre_conf).delete(pre_outputPath, true);
		Job pre_job = Job.getInstance(pre_conf, "Page Rank");
		//set mapper, combiner, reducer
		pre_job.setJarByClass(PageRank.class);
		pre_job.setMapperClass(PreMapper.class);
		//job.setCombinerClass(MyCombiner.class);
		pre_job.setReducerClass(PreReducer.class);
		//partion method is by default
		//job.setPartitionerClass(HashPartitioner.class);
		pre_job.setOutputKeyClass(Text.class);
		//value can be changed
		pre_job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(pre_job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(pre_job, pre_outputPath);
		flag = pre_job.waitForCompletion(true);

		long loadTime=System.currentTimeMillis();
		System.out.println("time to load data: " + (loadTime-startTime) + "ms");

		//set an iteration number first, and do not consider loss now
		int iterate_num = 0;
		String loop_in = loopFile, loop_out = loop2File;
		while(true)
		{
			if(iterate_num > loop_limit)
			{
				break;
			}
			iterate_num = iterate_num + 1;

			long it0Time=System.currentTimeMillis();

			//BETTER:if using many reducers, not only part-r-00000 can be produced
			//in such case, we should read from the whole directory, and remove the _SUCCESS first
			Configuration conf = new Configuration();
			conf.set("mapred.jar", "PageRank.jar");
			conf.set("factor", String.valueOf(factor));
			conf.set("size", String.valueOf(size));
			Path tpath = new Path(loop_out);
			tpath.getFileSystem(conf).delete(tpath, true);
			Job job = Job.getInstance(conf, "Page Rank");
			job.setJarByClass(PageRank.class);
			job.setMapperClass(PGMapper.class);
			//BETTER:local combine for ranks
			job.setReducerClass(PGReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(loop_in + "part-r-00000"));
			FileOutputFormat.setOutputPath(job, tpath);
			flag = job.waitForCompletion(true);

			//check if coverge: yes then goto end and project the value column; no then redistribute the loss and also only save new value
			Configuration check_conf = new Configuration();
			check_conf.set("mapred.jar", "PageRank.jar");
			//check_conf.set("epison", String.valueOf(epison));
			Path check_path = new Path(checkFile);
			check_path.getFileSystem(check_conf).delete(check_path, true);
			Job check_job = Job.getInstance(check_conf, "Page Rank");
			check_job.setNumReduceTasks(1);
			check_job.setJarByClass(PageRank.class);
			check_job.setMapperClass(CheckMapper.class);
			check_job.setReducerClass(CheckReducer.class);
			check_job.setOutputKeyClass(Text.class);
			check_job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(check_job, new Path(loop_out + "part-r-00000"));
			FileOutputFormat.setOutputPath(check_job, check_path);
			flag = check_job.waitForCompletion(true);
			//read 2 info from check file 
			double loss = 0, dist = 0;
			String path2 = checkFile + "/part-r-00000";
			String check_line;
			FileSystem check_fs = FileSystem.get(URI.create(path2), check_conf);
			FSDataInputStream check_stream = check_fs.open(new Path(path2));
			BufferedReader check_buffer = new BufferedReader(new InputStreamReader(check_stream));
			while((check_line = check_buffer.readLine()) != null)
			{
				String[] lines = check_line.split("\t");
				if(lines[0].equals("loss"))
				{
					loss = Double.parseDouble(lines[1]);
				}
			}
			check_buffer.close();
			check_stream.close();
			check_fs.close();

			//swap the in and out directory for next loop
			String tstr = loop_in;
			loop_in = loop_out;
			loop_out = tstr;

			//redistribute the loss
			Configuration loss_conf = new Configuration();
			loss_conf.set("mapred.jar", "PageRank.jar");
			Path loss_path = new Path(loop_out);
			loss_path.getFileSystem(loss_conf).delete(loss_path, true);
			loss_conf.set("factor", String.valueOf(factor));
			loss_conf.set("loss", String.valueOf(loss));
			loss_conf.set("size", String.valueOf(size));
			Job loss_job = Job.getInstance(loss_conf, "Page Rank");
			loss_job.setJarByClass(PageRank.class);
			loss_job.setMapperClass(LossMapper.class);
			loss_job.setReducerClass(LossReducer.class);
			loss_job.setOutputKeyClass(Text.class);
			loss_job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(loss_job, new Path(loop_in + "part-r-00000"));
			FileOutputFormat.setOutputPath(loss_job, loss_path);
			flag = loss_job.waitForCompletion(true);

			//compute the distance using newval after loss redistribution
			Configuration dist_conf = new Configuration();
			dist_conf.set("mapred.jar", "PageRank.jar");
			Path dist_path = new Path(checkFile);
			dist_path.getFileSystem(dist_conf).delete(dist_path, true);
			Job dist_job = Job.getInstance(dist_conf, "Page Rank");
			dist_job.setNumReduceTasks(1);
			dist_job.setJarByClass(PageRank.class);
			dist_job.setMapperClass(DistMapper.class);
			dist_job.setReducerClass(DistReducer.class);
			dist_job.setOutputKeyClass(Text.class);
			dist_job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(dist_job, new Path(loop_out + "part-r-00000"));
			FileOutputFormat.setOutputPath(dist_job, dist_path);
			flag = dist_job.waitForCompletion(true);
			//read dist info from check file 
			path2 = checkFile + "/part-r-00000";
			String dist_line;
			FileSystem dist_fs = FileSystem.get(URI.create(path2), dist_conf);
			FSDataInputStream dist_stream = dist_fs.open(new Path(path2));
			BufferedReader dist_buffer = new BufferedReader(new InputStreamReader(dist_stream));
			while((dist_line = dist_buffer.readLine()) != null)
			{
				String[] lines = dist_line.split("\t");
				if(lines[0].equals("dist"))
				{
					dist = Double.parseDouble(lines[1]);
				}
			}
			dist_buffer.close();
			dist_stream.close();
			dist_fs.close();

			System.out.println("iterate num: " + iterate_num);  
			System.out.println("loss: " + loss);  
			System.out.println("dist: " + dist);  
			
			//swap the in and out directory for next loop
			tstr = loop_in;
			loop_in = loop_out;
			loop_out = tstr;

			long it1Time=System.currentTimeMillis();
			System.out.println("iteration time: "+(it1Time - it0Time)+"ms");  

			//compare the dist with epson
			if(dist < epison)
			{
				System.out.println("already converge");
				break;
			}
		}
		long afterTime=System.currentTimeMillis();
		System.out.println("total loop time: "+(afterTime - loadTime)+"ms");  

		//End Job, just map the key and new rank, no reducer
		Configuration end_conf = new Configuration();
		end_conf.set("mapred.jar", "PageRank.jar");
		Path end_path = new Path(otherArgs[1]);
		end_path.getFileSystem(end_conf).delete(end_path, true);
		Job end_job = Job.getInstance(end_conf, "Page Rank");
		end_job.setJarByClass(PageRank.class);
		end_job.setMapperClass(EndMapper.class);
		end_job.setReducerClass(EndReducer.class);
		end_job.setMapOutputKeyClass(DoubleWritable.class);
		end_job.setMapOutputValueClass(Text.class);
		end_job.setOutputKeyClass(Text.class);
		end_job.setOutputValueClass(DoubleWritable.class);
		//define the sort method
		end_job.setSortComparatorClass(myComparator.class);
		FileInputFormat.addInputPath(end_job, new Path(loop_in + "part-r-00000"));
		FileOutputFormat.setOutputPath(end_job, end_path);
		flag = end_job.waitForCompletion(true);

		long endTime=System.currentTimeMillis();
		System.out.println("total time used: "+(endTime - startTime)+"ms");  
		System.exit(flag?0:1);
    }
}

