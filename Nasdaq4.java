package com.Rajesh.mapreduce;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Nasdaq4 {

public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
		
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			

		      
		      String stockColHeader = "stock_symbol" ;
		      
		      String line = value.toString();
		      String[] column = line.split(",");
		      
		      String stockSymbol = column[1] ;
		      String stockHigh   = column[4] ;
		      if(stockSymbol.compareTo(stockColHeader) != 0 ) {  
				    String stockYearMonth   = column[2].substring(0, 7);
		    		context.write(new Text(stockSymbol + "-" + stockYearMonth), new DoubleWritable(Double.parseDouble(stockHigh)));
		    	}
		}
		
	}

public static class MyCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		
		double high = 0;
		double currhigh = 0;
		
		for(DoubleWritable val : values) {
		    
			currhigh = val.get();
			
			if (currhigh > high) {
				high = currhigh ;
			}
			}
        context.write(key, new DoubleWritable(high));
		
		}
	
}
public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException{
		
		double high = 0;
		double currhigh = 0;
		
		for(DoubleWritable val : values) {
		    
			currhigh = val.get();
			
			if (currhigh > high) {
				high = currhigh ;
			}
			}
        context.write(key, new DoubleWritable(high));
		
		}
		
	}
		

	
public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Nasdaq4");
		
		job.setJarByClass(Nasdaq4.class);
		
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setCombinerClass(MyReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
		job.waitForCompletion(true);
	}
}
