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
//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//NOTICE: below are for loggers in map-reduce, need to set the log file in HADOOP mapred-site.xml
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BroadcastJoin extends Configured implements Tool
{	
	public static class BroadcastJoinMapper extends Mapper<Object, Text, Text, Text>
	{
		//Logger logger = LoggerFactory.getLogger(BroadcastJoinMapper.class);
		//NOTICE: this must be set static
		private static HashMap<String, ArrayList<String>> smallTable = null; // storage of the small table in memory.
		
		//before the map step, get the small table file from DistributedCache,
		//and load the records into the memory(HashMap smallTable).
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			//context.write(new Text("setup start"), new Text("1"));
			//logger.warn("now go to map setup");
			super.setup(context);
			if(context.getCacheFiles() == null || context.getCacheFiles().length <= 0)
			{
				//context.write(new Text("error when get cache"), new Text("1"));
				return;
			}
			smallTable = new HashMap<String, ArrayList<String>>();
			URI[] distributedPaths = context.getCacheFiles();
			
			for (URI p : distributedPaths)				
			{
				Path cacheFile = new Path(p.getPath());
				String cacheName = cacheFile.getName().toString();
				//logger.warn(" cache file name: " + cacheName);
				BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(cacheName),"utf-8"));
				String line;
				while ((line = reader.readLine()) != null)
				{
					//logger.warn("line: " + line);
					String[] record = line.split("\t");
					String joinKey = record[0];
						
					if (record.length < 2)
					{
						//logger.warn("error: the length of the record is too short.");
						continue;
					}

					String otherKeys = record[1];
					for (int i = 2; i < record.length; i++)
					{
						otherKeys = otherKeys + "\t" + record[i];
					}
						
					//build the hash map for small table R
					if (!smallTable.containsKey(joinKey))							
						smallTable.put(joinKey, new ArrayList<String>());
					smallTable.get(joinKey).add(otherKeys);
				}
				reader.close();
			}			
		}

		//for each map task, just check whether the joinKey of a record from the big table exists in the HashMap.
		//if exists, output the joined records.
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{	
			String[] record = value.toString().split("\t");
			if (record.length < 2)
			{
				//logger.warn("error: the length of the record is too short.");
				return;
			}
			
			String joinKey = record[0];
			
			if (!smallTable.containsKey(joinKey))
			{
				return;
			}
			
			String otherKeys = record[1];
			for (int i = 2; i < record.length; i++)
			{
				otherKeys = otherKeys + "\t" + record[i];
			}
			
			ArrayList<String> matchList = smallTable.get(joinKey);
			for (String m : matchList)
			{
				String mergeValue = m + "\t" + otherKeys;
				context.write(new Text(joinKey), new Text(mergeValue));
			}			
		}	
	}
	
	@Override
	public int run(String[] arg0) throws Exception
	{
		String hdfs = "hdfs://172.31.222.90:9000/user/hadoop/";
		//arg0[0] is the hdfs path of small table file;
		//arg0[1] is the hdfs path of big table file;
		//arg0[2] is the output path.
		
		Job job = Job.getInstance(this.getConf(),"BroadcastJoin");		
		Path cacheFile = new Path(hdfs + arg0[0]);
		job.addCacheFile(cacheFile.toUri());			// add the small table file into distributed cache file.
		System.err.println("cache file: " + cacheFile.toUri());
		
		job.setJarByClass(BroadcastJoin.class);
		
		job.setMapperClass(BroadcastJoinMapper.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(0); // do not need reduce step.
		
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
			int ret = ToolRunner.run(new BroadcastJoin(), args);
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

