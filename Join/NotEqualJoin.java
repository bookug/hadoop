import java.util.Collections;
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

/*
 * table_R:
 * age	teacher_name
 * 35	teacher_1
 * 48	teacher_2
 * 32	teacher_3
 * 55	teacher_4
 * ...	...
 * 
 * table_S:
 * age	student_name
 * 27	student_1
 * 39	student_2
 * 36	student_3
 * 19	student_4
 * ...	...
 * 
 * select * from table_R, table_S
 * NOTICE:the age of teacher should be greater than student:)
 * where table_R.age > table_S.age;
 */

public class NotEqualJoin extends Configured implements Tool
{
	
	public static class NotEqualJoinMapper extends Mapper<Object, Text, Text, Text>
	{
		private static ArrayList<Integer> tableRJoinKeys = null;
		
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
			
			String tableFlag = "";
			
			if (pathName.endsWith("R.dat"))			
			{
				tableFlag = "R";			
			}
			else if (pathName.endsWith("S.dat"))			
			{
				tableFlag = "S";			
			}
			else
			{
				//System.err.println("error: illegal table path name.");
				return;
			}
			
			String[] recordItems = value.toString().split("\t");
			if (recordItems.length < 2)
			{
				//System.err.println("error: the length of the record is too short.");
				return;
			}
			
			String joinKey = recordItems[0];
			String other = tableFlag + "\t" + value.toString();
			
			if (tableFlag.equals("S"))
			{
				int idx = this.binarySearch(Integer.parseInt(joinKey));
				for (int i = idx; i < tableRJoinKeys.size(); i++)
				{
					String outKey = tableRJoinKeys.get(i).toString();
					context.write(new Text(outKey), new Text(other));	
				}
			}
			else
			{
				context.write(new Text(joinKey), new Text(other));
			}
		}
		
		//find the first element's index which is bigger than key.
		public int binarySearch(Integer key)
		{
			int left = 0, right = tableRJoinKeys.size();
			
			while (left < right)
			{
				int mid = (left + right) / 2;
				if (tableRJoinKeys.get(mid) <= key)
					left = mid + 1;
				else
					right = mid;
			}
			
			return left;
		}
		
		//collect and sort the join key set of table R
		protected void setup(Context context) throws IOException
		{
			tableRJoinKeys = new ArrayList<Integer>();
			HashSet<Integer> tableRJoinKeySet = new HashSet<Integer>();
			
			URI[] distributedPaths = context.getCacheFiles();
			for (URI p : distributedPaths)				
			{
				Path cacheFile = new Path(p.getPath());
				String cacheName = cacheFile.getName().toString();
				BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(cacheName),"utf-8"));
				String line;
				while ((line = reader.readLine()) != null)
				{
					String[] recordItems = line.split("\t");
					Integer joinKey = Integer.parseInt(recordItems[0]);
					
					tableRJoinKeySet.add(joinKey);
				}
				reader.close();
			}
			
			for (Integer i : tableRJoinKeySet)
				tableRJoinKeys.add(i);
			Collections.sort(tableRJoinKeys);
		}		
	}
	
	public static class NotEqualJoinReducer extends Reducer<Text, Text, Text, Text>
	{
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			ArrayList<String> tableRRecords = new ArrayList<String>();
			ArrayList<String> tableSRecords = new ArrayList<String>();
			
			for (Text r : values)
			{
				String val = r.toString();
				String[] record = val.split("\t");
				String tableFlag = record[0];
				String other = record[1];
				for (int i = 2; i < record.length; i++)
				{
					other = other + "\t" + record[i];
				}

				if (tableFlag.equals("R"))				
				{
					tableRRecords.add(other);
				}
				
				else if (tableFlag.equals("S"))				
				{
					tableSRecords.add(other);				
				}
			}
			
			for (String r : tableRRecords)
			{
				for (String s : tableSRecords)
				{					
					context.write(new Text(r), new Text(s));
				}
			}	
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception
	{
		//arg0[0] is the path of table R keys file;
		//arg0[1] is the path of input file;	
		//arg0[2] is the output path.
		Job job = Job.getInstance(this.getConf(),"NotEqualJoin");
		job.addCacheFile(new Path(arg0[0]).toUri());
		
		job.setJarByClass(NotEqualJoin.class);
		
		job.setMapperClass(NotEqualJoinMapper.class);
		job.setReducerClass(NotEqualJoinReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		if (arg0.length >= 4)
		{
			int reducerNum = Integer.parseInt(arg0[3]);
			job.setNumReduceTasks(reducerNum);
		}		

		//FileInputFormat.addInputPath(job, new Path(arg0[0]));
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
			int ret = ToolRunner.run(new NotEqualJoin(), args);
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

