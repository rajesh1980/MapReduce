package com.Rajesh.mapreduce;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Nasdaq3A {

public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			

		      
		      String stockColHeader = "stock_symbol" ;
		      
		      String line = value.toString();
		      String[] column = line.split(",");
		      
		      String stockSymbol = column[1] ;
		      String stockVolume = column[7] ;
		      
		      
		      
		    	if(stockSymbol.compareTo(stockColHeader) != 0) {  
				    String stockYearMonth   = column[2].substring(0, 7);
				    context.write(new Text(stockSymbol), new LongWritable(Long.parseLong(stockVolume)));
				    context.write(new Text("@@@" + stockSymbol + "-" + stockYearMonth), new LongWritable(Long.parseLong(stockVolume)));
		    	}
		}
		
	}

public static class MyPartition extends Partitioner<Text, LongWritable>  {

	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	public int getPartition(Text key, LongWritable value, int numPartitions) {
		
		if (key.toString().substring(0, 3).matches("@@@"))
			return 0;
		else
			return 1;
	}

	
	
}

public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
		
		Text mykey = new Text(key.toString().replaceFirst("@@@", ""));
		
		long sum = 0;
		
		for(LongWritable val : values) {
		
			sum = sum + val.get() ;
			}
        context.write(mykey, new LongWritable(sum));
		
		}
		
	}
		
		

	
public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Nasdaq3A");
		
		job.setJarByClass(Nasdaq3A.class);
		job.setNumReduceTasks(2);
		
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setPartitionerClass(MyPartition.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.waitForCompletion(true);
	}
}
