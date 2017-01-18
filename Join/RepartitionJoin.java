import java.io.IOException;
import java.util.ArrayList;

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

public class RepartitionJoin extends Configured implements Tool
{
	//NOTICE:another strategy is to use text for value instead of Record
	public static class RepartitionJoinMapper extends Mapper<Object, Text, Text, Text>
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
			Text joinKey = new Text(recordItems[0]);
			String record = tableFlag;
			for (int i = 1; i < recordItems.length; i++)
			{
				record = record + "\t" + recordItems[i];
			}
			context.write(joinKey, new Text(record));	
		}
	}	

	public static class RepartitionJoinReducer extends Reducer<Text, Text, Text, Text>
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
		//Job job=new Job(this.getConf(),"ReduceSideJoin");
		Job job = Job.getInstance(this.getConf(),"RepartitionJoin");
		
		job.setJarByClass(RepartitionJoin.class);
		
		//NOTICE: no need to set combiner here
		job.setMapperClass(RepartitionJoinMapper.class);
		job.setReducerClass(RepartitionJoinReducer.class);
		
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
			int ret = ToolRunner.run(new RepartitionJoin(), args);
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

