//package org.apache.hadoop.examples;
//
//If not use package:
//yarn jar WordCount.jar WordCount input output
//hadoop jar WordCount.jar WordCount /input /output
//If using package(then a list of directories...)
//WordCount should be changed to other like org.apache.hadoop.examples.WordCount

//output need to be removed each time debore running, or you can do it in program
//all things in input/ will be dealed if not specified input/xxx

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount1
{
	//BETTER:place main logic  in mapper can lower the weight of reducers and the cost of communications

	//Mapper and Reducer are both templates, use specified types to init
	public static class TokMapper extends Mapper<Object, Text, Text, IntWritable>
	{
		//final in Java is used with class, method or variable
		//for class, other classes can not extends this class
		//for method, incase others modify its content
		//for variable, not-modified if basic type, not point to others if a pointer
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens())
			{
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			//System.out.println("Combiner输入分组<" + key.toString() + ",N(N>=1)>");
			int sum = 0; //maybe use long and LongWritable  is better
			for(IntWritable val : values)
			{
				//System.out.println("Combiner输入键值对<" + key.toString() + "," + val.get() + ">");
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
			//System.out.println("Combiner输出键值对<" + key.toString() + "," + sum + ">");
		}

	}

	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception
	{
		long startTime=System.currentTimeMillis();

		Configuration conf = new Configuration();
		//use this if run on windows
		//conf.set("mapred.job.tracker", "server90:9001");
		//NOTICE:below is needed, otherwise "ClassNotDef" error will be reported
		//we do not upload the jar file to hdfs, so in hdfs system will not find other classes needed
		//add this to tell it how to find the corresponding classes
		conf.set("mapred.jar", "WordCount1.jar");

		//Notice the arg order
		//java WordCount input output 
		//jar WordCount.jar WordCount input output
		//the first arg is WordCount, the second is input, the third is output
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2)
		{
			System.err.println("Usage: WordCount <input> <output>");
			System.exit(2);
		}

		//remove the output directory for this program first
		Path outputPath = new Path(otherArgs[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		Job job = Job.getInstance(conf, "Word Count");
		//set mapper, combiner, reducer
		job.setJarByClass(WordCount1.class);
		job.setMapperClass(TokMapper.class);
		//combiner is different from reducer because it only deal with pairs from one mapper
		//map->combine->shuffle(Partioner)->reduce(main loop)
		//job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(SumReducer.class);
		//partion method is by default
		//job.setPartitionerClass(HashPartitioner.class);

		//set IO communication
		//key should not be modified
		job.setOutputKeyClass(Text.class);
		//value can be changed
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		//submit the job and run map-reduce
		boolean flag = job.waitForCompletion(true);

		long endTime=System.currentTimeMillis();
		System.out.println("total time used: "+(endTime - startTime)+"ms");  

		System.exit(flag?0:1);
	}
}

