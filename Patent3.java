package com.Rajesh.mapreduce;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Patent3 {

	public static class PatentMap extends Mapper<LongWritable,Text, IntWritable, Text> {
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
			String[] column = value.toString().split(",");
			String patent = column[0] ;
			try {
					int asignee = Integer.parseInt(column[6]) ;
					context.write(new IntWritable(asignee), new Text("PAT" +"\t" + patent));
			}
			catch (NumberFormatException e) {
				System.out.println("Number Format Exception" + e); 
			}
			
		}
		
	}

	public static class AsigneeMap extends Mapper<LongWritable,Text, IntWritable, Text> {
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] column = value.toString().split(",");
			String coname = column[1];
			
			try {
				int asignee = Integer.parseInt(column[0]) ;
				context.write(new IntWritable(asignee), new Text("ASN"+ "\t" + coname));
			}
			catch (NumberFormatException e) {
			System.out.println("Number Format Exception" + e); 
			}			
			
		}
		
	}

	public static class MyReducer extends Reducer<IntWritable, Text, Text, Text> {
		
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			Text coname = new Text() ;
			ArrayList<String> patents = new ArrayList<String>() ;
			
			for (Text value : values) {
				String [] column = value.toString().split("\t") ;
				if (column[0].equals("PAT")) {
					patents.add(column[1]);
				} else {
					coname.set(column[1]);
				}
			}
			
			for (String patent : patents) {
				context.write(new Text(patent), coname);
			}
		}
	}
	public static void main(String[] args)  throws IOException, ClassNotFoundException, InterruptedException{
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Patent3");
		job.setJarByClass(Patent3.class);
		job.setReducerClass(MyReducer.class) ;
//		job.setNumReduceTasks(0);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PatentMap.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AsigneeMap.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
	
		job.waitForCompletion(true);
	}

}


