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
import java.util.TreeMap;
import java.util.List; 
import java.util.ArrayList; 
import java.util.Set;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Iterator;
//import java.util.AbstractMap;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//DEBUG:it is hard to debug hadoop program because system output in map-reduce not seen
//http://www.aboutyun.com/thread-7682-1-1.html
//http://jhao6000.blog.163.com/blog/static/1746473892013920113841247/
//maybe we can write to file

//NOTICE:this program only achieve basic version of inverted index for a vocabuly
//The order is on character order for doc id, which maybe not so good
public class InvertedIndex1
{
	//BETTER:place main logic  in mapper can lower the weight of reducers and the cost of communications
	//MAYBE:us elong and LongWritable if data is large

	//Mapper and Reducer are both templates, use specified types to init
	public static class NewMapper extends Mapper<Object, Text, Text, Text>
	{
		private static HashMap<String, Integer> asso;  //int can not be used here
		private Text word = new Text();
		private Text keyInfo = new Text();
		private Text valueInfo = new Text();

		public void run(Context context) throws IOException, InterruptedException 
		{  
			setup(context);//只运行一次，可以重载实现自己的功能，比如获得Configuration中的参数  
			while (context.nextKeyValue()) {  
				map(context.getCurrentKey(), context.getCurrentValue(), context);  
			}  
			cleanup(context);  
		} 

		//Called once at the beginning of the task.
		public void setup(Context context) throws IOException, InterruptedException 
		{
			asso = new HashMap<String, Integer>();  //int can not be used here
		}

		//Called once at the beginning of the task.
		public void cleanup(Context context) throws IOException, InterruptedException 
		{
			FileSplit fileSplit = (FileSplit)context.getInputSplit();
			String fileName = fileSplit.getPath().getName();
			Iterator<Map.Entry<String, Integer>> mit = asso.entrySet().iterator();
			while(mit.hasNext())
			{
				Map.Entry<String, Integer> entry = mit.next();
				keyInfo.set(entry.getKey());
				valueInfo.set(fileName + ":" + entry.getValue());
				context.write(keyInfo, valueInfo);
			}
		}

		//the key is a doc id
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String s = new String();
			StringTokenizer itr = new StringTokenizer(value.toString());
			//InputSplit inputSplit=(InputSplit)context.getInputSplit();
			//String filename=((FileSplit)inputSplit).getPath().getName();
			//split = (FileSplit)context.getInputSplit();
			
			while(itr.hasMoreTokens())
			{
				s = itr.nextToken();
				Integer v = 0;
				if(asso.containsKey(s))
				{
					v = asso.get(s);
				}
				asso.put(s, v+1);
			}
		}
	}

	public static class NewReducer extends Reducer<Text, Text, Text, Text>
	{
		private Text valueInfo = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			//sort via map, using treemap instead of hashmap here
			Map<String, Integer> result = new TreeMap<String, Integer>();
			for(Text val : values)
			{
				//StringTokenizer st = new StringTokenizer(str, "- :");
				String[] str = val.toString().split(":");
				//int num = Integer.parseInt(str[1]);
				String doc = str[0];
				int v = Integer.parseInt(str[1]);
			//NOTICE:maybe not only one value, because a file can be split into several pieces
			//The reason is that a real file maybe divided into several splits, each mapper deal with one split
			//so for given term+name, here may be several values
				if(result.containsKey(doc))
				{
					v = v + result.get(doc);
				}
				//NOTICE:the put method will substitute the original existing one directly
				result.put(doc, v);
			}

			//add to valueInfo
			String s = new String();
			for (Iterator<String> it = result.keySet().iterator(); it.hasNext();) 
			{
				String doc = it.next();
				//NOTICE:not use two it.next() here,
				//otherwise error will come, no such element exception
				s = s + doc + ":" + result.get(doc) + ",";
			}
			valueInfo.set(s);
			context.write(key, valueInfo);
		}
	}

	public static void main(String[] args) throws Exception
	{
		long startTime=System.currentTimeMillis();

		Configuration conf = new Configuration();
		//conf.setStrings("job_parms", "aaabbc");
		//use this if run on windows
		//conf.set("mapred.job.tracker", "server90:9001");
		//NOTICE:below is needed, otherwise "ClassNotDef" error will be reported
		//we do not upload the jar file to hdfs, so in hdfs system will not find other classes needed
		//add this to tell it how to find the corresponding classes
		conf.set("mapred.jar", "InvertedIndex1.jar");

		//Notice the arg order
		//java WordCount input output 
		//jar WordCount.jar WordCount input output
		//the first arg is WordCount, the second is input, the third is output
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2)
		{
			System.err.println("Usage: InvertedIndex1 <input> <output>");
			System.exit(2);
		}

		//remove the output directory for this program first
		Path outputPath = new Path(otherArgs[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		Job job = Job.getInstance(conf, "Inverted Index");
		//set mapper, combiner, reducer
		job.setJarByClass(InvertedIndex1.class);
		job.setMapperClass(NewMapper.class);
		//combiner is different from reducer because it only deal with pairs from one mapper
		//map->combine->shuffle(Partioner)->reduce(main loop)
		//In most cases, combiner is same as reducer
		//not all cases, that combiner should be used, for example, max is ok, mean is not
		//job.setCombinerClass(SumReducer.class);
		//if not use reducer, set to None:
		//job.setNumReduceTasks(0);
		//or just neglect, then hadoop will use default reducer: do nothing
		job.setReducerClass(NewReducer.class);
		//partion method is by default
		//job.setPartitionerClass(HashPartitioner.class);

		//set IO communication
		//key should not be modified
		job.setOutputKeyClass(Text.class);
		//value can be changed
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		//submit the job and run map-reduce
		boolean flag = job.waitForCompletion(true);

		long endTime=System.currentTimeMillis();
		System.out.println("total time used: "+(endTime - startTime)+"ms");  

		System.exit(flag?0:1);
	}
}

