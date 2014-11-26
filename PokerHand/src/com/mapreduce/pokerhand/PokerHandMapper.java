package com.mapreduce.pokerhand;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

public class PokerHandMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, IntWritable> {

	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void configure(JobConf job) {
		System.out.println("Made it into the configure method.  Load the file here!");
	}

	public void map(LongWritable key, Text value,
		OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		  String line = value.toString();
		  StringTokenizer itr = new StringTokenizer(line.toLowerCase());
		  while(itr.hasMoreTokens()) {
			  word.set(itr.nextToken());
			  output.collect(word, one);
		  }
	}

	public void cleanup() {
	}
}
