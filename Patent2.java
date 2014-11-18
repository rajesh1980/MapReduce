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


public class Patent2 {

	public static class MyMapper extends Mapper<LongWritable,Text, Text, Text> {
		
		//Declare HashMap to store cached Dept file for Patent2
		private Map<Integer, String> ClassMap = new HashMap<Integer, String>() ;
		
		protected void setup(Context context) throws IOException {
			
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			for (Path p : files) {
				if(p.getName().equals("Classes.txt")) {
					BufferedReader br = new BufferedReader(new FileReader(p.toString()));
					String line = br.readLine() ;
					
					while(line != null) {
				    String[] column = line.split("\t");
					
				    try {
				    int nclass = Integer.parseInt(column[0]);
					String title = column[1];
					ClassMap.put(nclass, title) ;
				    }
				    catch (NumberFormatException e) {
				    	System.out.println("Number format exception 1" + e);
				    }
					line = br.readLine() ;
					}
					br.close();
				}
				
			}
			

			if (ClassMap.isEmpty()) {
				throw new IOException("Unable to load Dept data.");
			}
		
			
		}
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
		    String line = value.toString();
		    String[] column = line.split(",");
			String patent = column[0];
			try {
			int nclass = Integer.parseInt(column[9]);
			if (ClassMap.containsKey(nclass)){
				context.write(new Text(patent), new Text(ClassMap.get(nclass))) ;
				}
			}
			catch(NumberFormatException e) {
				System.out.println("Number format exception 2" + e);
			}
			}
	}
	
	
	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Patent2");
		job.setJarByClass(Patent2.class);
		job.setNumReduceTasks(0);
		
	    try{
	        DistributedCache.addCacheFile(new URI("/user/cloudera/Rajesh/Lookup/Classes.txt"), job.getConfiguration());
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


