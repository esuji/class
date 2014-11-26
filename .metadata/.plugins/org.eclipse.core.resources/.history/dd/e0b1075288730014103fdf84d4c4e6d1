package com.mapreduce.pokerhand;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapred.JobTracker;

public class Driver {

	public static void main(String[] args) {
		// Arg0 : Training Set Path
		// Arg1 : Test Set Path
		// Arg2 : Output Path	
		String sTrainingSet = args[0];
		String sTestSet = args[1];
		String sOutput = args[2];
		
		JobClient client = new JobClient();
		JobConf conf = new JobConf(com.mapreduce.pokerhand.Driver.class);
		conf.setJobName("PokerHand");
  
		// specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		  
		// specify input and output directories
		//FileInputFormat.setInputPaths(conf, new Path(sInput));
		FileInputFormat.addInputPath(conf, new Path(sTestSet));
		FileOutputFormat.setOutputPath(conf, new Path(sOutput));
		
		// specify a MapReduce Mapper Class
		conf.setMapperClass(com.mapreduce.pokerhand.PokerHandMapper.class);
		// specify a reducer
		conf.setReducerClass(com.mapreduce.pokerhand.PokerHandReducer.class);
		conf.setCombinerClass(com.mapreduce.pokerhand.PokerHandReducer.class);	//Is this needed with only 1 node?

		conf.set("traindata", sTrainingSet);
		
		/*
		//-------------------------------------------------
		// Create a new Job
		Job job = new Job(conf);
		job.setJarByClass(com.mapreduce.pokerhand.Driver.class);
		// Specify various job-specific parameters     
		job.setJobName("myjob");
		job.setInputPath(new Path(sTestSet));
		job.setOutputPath(new Path(sOutput));
		job.setMapperClass(com.mapreduce.pokerhand.PokerHandMapper.class);
		job.setReducerClass(com.mapreduce.pokerhand.PokerHandReducer.class);
		job.getConfiguration().set("traindata", "poker-hand-trainset.data");
		// Submit the job, then poll for progress until the job is complete
		job.waitForCompletion(true);
		//-------------------------------------------------
		*/
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
