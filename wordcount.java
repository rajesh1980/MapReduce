/*******************************************************************************
   For succesfully running this project we need the hadoop common and the mapred jars which should be in the lib directory of the hadoop install
  ******************************************************************************/

package com.Rajesh.mapreduce;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

/**
* @author Rajesh Ramachandran 
* @version 1.0
* @since 15-Nov-2014
* @package com.Rajesh.mapreduce
* The WordCount program counts the number of times each word has appeared in the input text file, 
* it also calculates the length of the word. 
* We write a map reduce code to achieve this, where mapper makes key value pair from the input 
 * file and reducer does aggregation on this key value pair. We also have a combiner and partitioner 
 * written to perform an intermediate reduce and split the final reduce to 4 tasks.  
 */
public class wordcount {
	
	/** 
	 * @author Rajesh Ramachandran
	 * @interface Mapper
	 * <p>Map class is static and extends MapReduceBase and implements Mapper 
	 * interface having four hadoop generics type LongWritable, Text, IntWritable,
	 * Text
	 */

  public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      while (tokenizer.hasMoreTokens()) {
    	  String word = tokenizer.nextToken().toString().replaceAll("\\P{Alnum}", "");
        output.collect(new Text(word), new IntWritable(1));
      }
    }
  }
  
  public static class Combine implements Reducer<Text, IntWritable, Text, IntWritable> {


	@Override
	public void reduce(Text key, Iterator<IntWritable> values,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
	    String mykey = key.toString().toUpperCase();
    	
	    int sum = 0;
    	
        while (values.hasNext()) { 
        	sum = sum + values.next().get(); 

        } 
        
	    output.collect(new Text(mykey), new IntWritable(sum));
		
		
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	  
  } 

  public static class Partition implements Partitioner<Text, IntWritable> {


	@Override
	public int getPartition(Text key, IntWritable value, int numpartitions) {
		

		int keylen = key.toString().length();
		
		if (keylen <= 5) return 0;
		else if (keylen <= 7) return 1;
		     else if (keylen <= 10) return 2;
		          else  return 3;
		
	}

	@Override
	public void configure(JobConf arg0) {

		
	}
	
	
	  
  }
  
  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
    	
    	int sum = 0;
    	
        while (values.hasNext()) { 
        	sum = sum + values.next().get(); 
        } 
    	output.collect(new Text("<" + key.toString()+ " - " + key.toString().length() + ">" ), new IntWritable(sum));
    }
  }

  
  
  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(wordcount.class);
    conf.setJobName("wordcount");

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setNumReduceTasks(4);

    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);
    conf.setPartitionerClass(Partition.class);
    conf.setCombinerClass(Combine.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);
    

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }
}