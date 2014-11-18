package com.Rajesh.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Join {

	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
		
		//Declare HashMap to store cached Dept file for join
		private Map<String, String> deptMap = new HashMap<String, String>() ;
		
		protected void setup(Context context) throws IOException {
			
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			for (Path p : files) {
				if(p.getName().equals("Dept.txt")) {
					BufferedReader br = new BufferedReader(new FileReader(p.toString()));
					String line = br.readLine() ;
					while(line != null) {
					String[] column = line.split(",");
					deptMap.put(column[0], column[1]) ;
					line = br.readLine() ;
					}
					br.close();
				}
				
			}
			
			if (deptMap.isEmpty()) {
				throw new IOException("Unable to load Dept data.");
			}
		
			
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
				
			String[] column = value.toString().split(",") ;
			context.write(new Text(column[0]), new Text(deptMap.get(column[1]))) ;
		}
	}
	
	
	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Join");
		job.setJarByClass(Join.class);
		job.setNumReduceTasks(0);
		
	    try{
	        DistributedCache.addCacheFile(new URI("/user/cloudera/Rajesh/Lookup/Dept.txt"), job.getConfiguration());
	        }catch(Exception e){
	        	System.out.println(e);
	        }
		
		job.setMapperClass(MyMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.waitForCompletion(true);
	}

}


