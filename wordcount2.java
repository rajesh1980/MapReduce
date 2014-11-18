package com.Rajesh.mapreduce;


import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wordcount2 {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			
		      String line = value.toString();
		      StringTokenizer tokenizer = new StringTokenizer(line);
		      while (tokenizer.hasMoreTokens()) {
		    	  String word = tokenizer.nextToken().toString().replaceAll("\\P{Alnum}", "");
		    	  context.write(new Text(word), new IntWritable(1));
			
		}
		
	}
		
	}
		public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
			
			public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
				
				int sum = 0;
				
				for(IntWritable val : values) {
				
					sum = sum + val.get() ;
					}
		        context.write(key, new IntWritable(sum));
				
				}
				
			}
		
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcount2");
		
		job.setJarByClass(wordcount2.class);
		
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.waitForCompletion(true);
	}
}
