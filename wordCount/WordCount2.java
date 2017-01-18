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
import java.lang.String;
//import java.lang.Object;
import java.util.HashMap;
import java.util.Set;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Iterator;
//import java.util.AbstractMap;

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

public class WordCount2
{
	//BETTER:place main logic  in mapper can lower the weight of reducers and the cost of communications
	//MAYBE:us elong and LongWritable if data is large

	//Mapper and Reducer are both templates, use specified types to init
	public static class TokMapper extends Mapper<Object, Text, Text, IntWritable>
	{
	//We implement the main logic in mapper now
	//i.e. in-mapper-combine
	//need reducers, this counts for entire document

		//final in Java is used with class, method or variable
		//for class, other classes can not extends this class
		//for method, incase others modify its content
		//for variable, not-modified if basic type, not point to others if a pointer
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			HashMap<String, Integer> asso = new HashMap<String, Integer>();  //int can not be used here
			String s = new String();
			StringTokenizer itr = new StringTokenizer(value.toString());
			
			while(itr.hasMoreTokens())
			{
				s = itr.nextToken();
				Integer v = 0;
				if(asso.containsKey(s))
				{
					v = asso.get(s);
				}
				asso.put(s, v+1);
				//word.set(itr.nextToken());
				//write to disk, not in memory, not modify one or occupy the space
				//context.write(word, one);
			}

			//travesal teh hash map
			Iterator<Map.Entry<String, Integer>> it = asso.entrySet().iterator();
			while(it.hasNext())
			{
				Map.Entry<String, Integer> entry = it.next();
				word.set(entry.getKey());
				IntWritable num = new IntWritable(entry.getValue());
				context.write(word, num);
			}
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
		conf.set("mapred.jar", "WordCount2.jar");

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
		job.setJarByClass(WordCount2.class);
		job.setMapperClass(TokMapper.class);
		//combiner is different from reducer because it only deal with pairs from one mapper
		//map->combine->shuffle(Partioner)->reduce(main loop)
		//In most cases, combiner is same as reducer
		//not all cases, that combiner should be used, for example, max is ok, mean is not
		//job.setCombinerClass(SumReducer.class);
		//if not use reducer, set to None:
		//job.setNumReduceTasks(0);
		//or just neglect, then hadoop will use default reducer: do nothing
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

