import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.List; 
import java.util.Vector; 
import java.util.Set;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.Collections;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class ImprovedRepartitionJoin extends Configured implements Tool
{
	//NOTICE:another strategy is to use text for value instead of Record
	public static class ImprovedRepartitionJoinMapper extends Mapper<Object, Text, Text, Text>
	{
		//for each map task, tag the records with "R" or "S",
		//and output by their join keys.
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
			String tableFlag = "";
			
			//NOTICE:the pathName is the full path name, i.e. hdfs://....//table0R.txt
			if (pathName.endsWith("R.dat"))
			{
				tableFlag = "R";
			}
			else if (pathName.endsWith("S.dat"))
			{
				tableFlag = "S";
			}
			//else
			//{
				//System.err.println("error: illegal table path name.");
				//return;
			//}
			
			String[] recordItems = value.toString().split("\t");
			if (recordItems.length < 2)
			{
				System.err.println("error: the length of the record is too short.");
				return;
			}
			
			//the 0-th one is always the join key
			//NOTICE: if has several join keys, should rewrite
			Text joinKey = new Text(recordItems[0] + "\t" + tableFlag);  //R < S
			String record = recordItems[1];
			for (int i = 2; i < recordItems.length; i++)
			{
				record = record + "\t" + recordItems[i];
			}
			context.write(joinKey, new Text(record));	
		}
	}	

	public static class ImprovedRepartitionJoinReducer extends Reducer<Text, Text, Text, Text>
	{
		private static ArrayList<String> tableROtherKeys;
		private static String prevKey;

		public void setup(Context context) throws IOException, InterruptedException 
		{
			tableROtherKeys = new ArrayList<String>();
			prevKey = "";
		}

		//NOTICE:the same key will be sorted together, and partition is after sorting process
		//if reducer num is 1, then all will be partitioned into the same reducer  (default reducer num is 1)
		//In addition, Iterable is not in memory but in disk and sorted, so read sequentially just like iterable~
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			String[] record = key.toString().split("\t");
			String realKey = record[0];
			String tag = record[1];
			//NOTICE:this should be placed here, because some S keys not in mapping maybe cause error i.e. 1R 1S 2S
			//Another case is 1R 2S  (i.e. no 1S available)
			if(!realKey.equals(prevKey))
			{
				tableROtherKeys.clear();
				prevKey = realKey;
			}

			//NOTICE:one worry is that the list may be too large to be placed  in memory!
			//So we just place the whole smaller table in memory, and ensure that all r arrive before s and they go to teh same partition
			//ArrayList<String> tableSOtherKeys = new ArrayList<String>();
			if (tag.equals("R"))
			{
				for (Text val : values)
				{
					String otherKeys = val.toString();
					tableROtherKeys.add(otherKeys);
				}
			}
			else //tag is S
			{
				for (Text val : values)
				{
					String s = val.toString();
					for (String r : tableROtherKeys)
					{
						Text res = new Text(r + "\t" + s);
						context.write(new Text(realKey), res);
					}	
				}
			}
		}
	}

//NOTICE:we must use partioner to ensure the correctness
//the same key go to the same reduce(), and the same first-part-key go to the same reducer!
	public static class ImprovedRepartitionJoinPartitioner extends HashPartitioner<Text, Text>
	{
		//Partitioner get input from mapper, so here no iterable
		public int getPartition(Text key, Text value, int numReduceTasks)
		{
			String term = key.toString().split("\t")[0];
			return super.getPartition(new Text(term), value, numReduceTasks);
		}
	}

	@Override
	public int run(String[] arg0) throws Exception 
	{
		//Job job=new Job(this.getConf(),"ReduceSideJoin");
		Job job = Job.getInstance(this.getConf(),"ImprovedRepartitionJoin");
		
		job.setJarByClass(ImprovedRepartitionJoin.class);
		
		//NOTICE: no need to set combiner here
		job.setMapperClass(ImprovedRepartitionJoinMapper.class);
		job.setReducerClass(ImprovedRepartitionJoinReducer.class);
		job.setPartitionerClass(ImprovedRepartitionJoinPartitioner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		 //set the number of reducers by argument. 
		if (arg0.length >= 3)
		{
			int reducerNum = Integer.parseInt(arg0[2]);
			job.setNumReduceTasks(reducerNum);
		}		

		//arg0[0] is the input path		 
		//arg0[1] is the output path.
		FileInputFormat.addInputPath(job, new Path(arg0[0]));	
		Path output = new Path(arg0[1]);
		output.getFileSystem(this.getConf()).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);
		
		job.waitForCompletion(true);

		return job.isSuccessful()?0:1;
	}
	
	public static void main(String[] args)
	{
		try
		{
			long startTime = System.currentTimeMillis();
			int ret = ToolRunner.run(new ImprovedRepartitionJoin(), args);
			long endTime = System.currentTimeMillis();
			System.err.println("total time used: " + (endTime-startTime) + "ms");
			System.exit(ret);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}

