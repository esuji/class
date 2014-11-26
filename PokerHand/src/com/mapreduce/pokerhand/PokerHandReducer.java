package com.mapreduce.pokerhand;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PokerHandReducer extends MapReduceBase
    implements Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text key, Iterator<IntWritable> values,
		OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		// process values
		int sum = 0;
	    while (values.hasNext()) {
	      IntWritable value = (IntWritable) values.next();
	      sum += value.get(); // add the next count to the sum for that word
	    }

	    output.collect(key, new IntWritable(sum));
	}

}
