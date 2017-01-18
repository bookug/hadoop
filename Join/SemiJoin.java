import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.net.URI;

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
//NOTICE: below are for loggers in map-reduce, need to set the log file in HADOOP mapred-site.xml
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SemiJoin extends Configured implements Tool
{
	//NOTICE:another strategy is to use text for value instead of Record
	public static class SemiJoinMapper extends Mapper<Object, Text, Text, Text>
	{
		private static HashSet<String> smallTableKeys = null; 
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
			if(context.getCacheFiles() == null || context.getCacheFiles().length <= 0)
			{
				//context.write(new Text("error when get cache"), new Text("1"));
				return;
			}
			smallTableKeys = new HashSet<String>();
			URI[] distributedPaths = context.getCacheFiles();
			
			for (URI p : distributedPaths)				
			{
				Path cacheFile = new Path(p.getPath());
				String cacheName = cacheFile.getName().toString();
				BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(cacheName),"utf-8"));
				String line;
				while ((line = reader.readLine()) != null)
				{
					String joinKey = line;
					//build the hash set for small table R's keys
					if (!smallTableKeys.contains(joinKey))							
						smallTableKeys.add(joinKey);
				}
				reader.close();
			}			
		}
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
			//else   //key file here
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
			String joinKey = recordItems[0];
			//another big table, should check key here, avoid sending out impossible pairs
			if(tableFlag.equals("S") && !smallTableKeys.contains(joinKey))  
			{
				return;
			}

			String record = tableFlag;
			for (int i = 1; i < recordItems.length; i++)
			{
				record = record + "\t" + recordItems[i];
			}
			context.write(new Text(joinKey), new Text(record));	
		}
	}	

	public static class SemiJoinReducer extends Reducer<Text, Text, Text, Text>
	{
		//for each reduce task, divide the values into two array list by their file tags,
		//then cartesian and output the two lists.
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			//NOTICE:one worry is that the list may be too large to be placed  in memory!
			ArrayList<String> tableROtherKeys = new ArrayList<String>();
			ArrayList<String> tableSOtherKeys = new ArrayList<String>();
			
			for (Text val : values)
			{
				String[] record = val.toString().split("\t");
				String tag = record[0];
				String otherKeys = record[1];
				for (int i = 2; i < record.length; i++)
				{
					otherKeys = otherKeys + "\t" + record[i];
				}
				
				if (tag.equals("R"))
				{
					tableROtherKeys.add(otherKeys);
				}
				else if (tag.equals("S"))
				{
					tableSOtherKeys.add(otherKeys);
				}
			}
			
			for (String r : tableROtherKeys)
			{
				for (String s : tableSOtherKeys)
				{					
					Text res = new Text(r + "\t" + s);
					context.write(key, res);
				}
			}	
		}
	}

	@Override
	public int run(String[] arg0) throws Exception 
	{
		String hdfs = "hdfs://172.31.222.90:9000/user/hadoop/";
		//arg0[0] is the hdfs path of small table keys;
		//arg0[1] is the hdfs path of table files
		//arg0[2] is the output path.
		
		Job job = Job.getInstance(this.getConf(),"SemiJoin");		
		//NOTICE:the smaller table's keys can be placed into buffer
		Path cacheFile = new Path(hdfs + arg0[0]);
		job.addCacheFile(cacheFile.toUri());
		System.err.println("cache file: " + cacheFile.toUri());
		
		job.setJarByClass(SemiJoin.class);
		
		job.setMapperClass(SemiJoinMapper.class);
		job.setReducerClass(SemiJoinReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//NOTICE: if add 2 inputs, then they will be dealed as two jobs??  no no
		//FileInputFormat.addInputPath(job, new Path(arg0[1]));	
		//FileInputFormat.addInputPath(job, new Path(arg0[2]));	
		//Path output = new Path(arg0[3]);
		FileInputFormat.addInputPath(job, new Path(arg0[1]));	
		Path output = new Path(arg0[2]);
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
			int ret = ToolRunner.run(new SemiJoin(), args);
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

