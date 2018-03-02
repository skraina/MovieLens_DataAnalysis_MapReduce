package com.github.skraina.movielens.rankgenre;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;


public class MovieLensRankGenreDriver extends Configured implements Tool
{
	public int run(String args[]) throws Exception
	{
		JobControl jobControl = new JobControl("MovieLensGenreByAge");
		Configuration conf1 = getConf();
		
		// Set FileIndex for input files
		//conf1.setInt("users-tiny.dat", 1);
		//conf1.setInt("ratings-tiny.dat", 2);
		conf1.setInt("users.dat", 1);
		conf1.setInt("ratings.dat", 2);

		Job job1 = Job.getInstance(conf1);
		//job1.getCounters();
		job1.setJobName("Movie Lens join Rating Users");
		job1.setJarByClass(MovieLensRankGenreDriver.class);
		
		job1.setMapperClass(MovieLensRankGenreMapper.class);
		job1.setReducerClass(MovieLensRankGenreReducer.class);
		job1.setSortComparatorClass(SortingComparatorUID.class);
		job1.setPartitionerClass(PartitionerUID.class);
		job1.setGroupingComparatorClass(GroupingComparatorUID.class);
		
		job1.setNumReduceTasks(1);
		job1.setMapOutputKeyClass(CompositeKeyWritableUID.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleOutputs.addNamedOutput(job1, "ParsedRecords", TextOutputFormat.class , Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job1, "BadRecords", TextOutputFormat.class , Text.class, Text.class);

		//FileInputFormat.addInputPath(job1, new Path(args[0] + "/users-tiny.dat"));
		//FileInputFormat.addInputPath(job1, new Path(args[0] + "/ratings-tiny.dat"));
		FileInputFormat.addInputPath(job1, new Path(args[0] + "/users.dat"));
		FileInputFormat.addInputPath(job1, new Path(args[0] + "/ratings.dat"));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/RatingsJoinUsers"));

		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(job1);
		jobControl.addJob(controlledJob1);
	
/*************************************************************************************************/
		
		Configuration conf2 = getConf();
		
		//Set FileIndex for the third data set
		//conf2.setInt("movies-tiny.dat", 3);
		conf2.setInt("movies.dat", 3);
		
		Job job2 = Job.getInstance(conf2);
		//job2.getCounters();
		job2.setJobName("Movie Lens join Movie AgeOccp-Rating");
		job2.setJarByClass(MovieLensRankGenreDriver.class);
		
		
		job2.setMapperClass(MovieLensRankGenreByAgeOccpMapper.class);
		job2.setCombinerClass(MovieLensRankGenreByAgeOccpCombiner.class);
		//job2.setCombinerKeyGroupingComparatorClass(CombinerKeyGroupingComparatorUID.class);

		job2.setReducerClass(MovieLensRankGenreByAgeOccpReducer.class);
		// Using this sortComparator is NOT beneficial as it allows to combine all ratings for a movieID
		// irrespective of the AG and OC groups/codes.
		//job2.setSortComparatorClass(SortingComparatorUID.class); // Datasets will be joined on Movie ID
		job2.setPartitionerClass(PartitionerUID.class);
		job2.setGroupingComparatorClass(GroupingComparatorUID.class);
		
		job2.setNumReduceTasks(1);
		job2.setMapOutputKeyClass(CompositeKeyWritableUID.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		//job2.setOutputKeyClass(CompositeKeyWritableUID.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		MultipleOutputs.addNamedOutput(job2, "ParsedRecords", TextOutputFormat.class , Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job2, "BadRecords", TextOutputFormat.class , Text.class, Text.class);

		//FileInputFormat.addInputPath(job2, new Path(args[0] + "/movies-tiny.dat"));
		FileInputFormat.addInputPath(job2, new Path(args[0] + "/movies.dat"));
		FileInputFormat.addInputPath(job2, new Path(args[1] + "/RatingsJoinUsers"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/AgeOccpGenres"));
		
		ControlledJob controlledJob2 = new ControlledJob(conf2);
		controlledJob2.setJob(job2);
		
		//chaining job1 to job2
		controlledJob2.addDependingJob(controlledJob1);
		jobControl.addJob(controlledJob2);

		//***************************************************************************//
		Configuration conf3 = getConf();
		Job job3 = Job.getInstance(conf3);
		//job3.getCounters();
		job3.setJobName("Movie Lens Rank Genre by Age Occupation");
		job3.setJarByClass(MovieLensRankGenreDriver.class);
		
		job3.setMapperClass(MovieLensRankGenreFinalMapper.class);
		job3.setCombinerClass(MovieLensRankGenreFinalCombiner.class);
		job3.setNumReduceTasks(1);
		job3.setReducerClass(MovieLensRankGenreFinalReducer.class);
		
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		//job3.setMapOutputValueClass(IntWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job3, new Path(args[1] + "/AgeOccpGenres"));
		FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/GenreRank"));
		
		ControlledJob controlledJob3 = new ControlledJob(conf3);
		controlledJob3.setJob(job3);
		
		//chaining job2 to job3
		controlledJob3.addDependingJob(controlledJob2);
		jobControl.addJob(controlledJob3);

		//***************************************************************************//*
		Configuration conf4 = getConf();
		Job job4 = Job.getInstance(conf4);
		//job4.getCounters();
		job4.setJobName("Movie Lens Rank Genre by Age Occupation Sorted");
		job4.setJarByClass(MovieLensRankGenreDriver.class);
		
		job4.setMapperClass(MovieLensRankGenreResultSortMapper.class);
		job4.setSortComparatorClass(SortingComparatorAGOC.class);
		job4.setGroupingComparatorClass(GroupingComparatorAGOC.class);
		job4.setNumReduceTasks(1);
		job4.setReducerClass(MovieLensRankGenreResultSortReducer.class);
		
		job4.setMapOutputKeyClass(CompositeKeyWritableAGOC.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(Text.class);
		//job4.setOutputKeyClass(CompositeKeyWritableAGOC.class);
		job4.setOutputValueClass(Text.class);

		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job4, new Path(args[1] + "/GenreRank"));
		FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/SortedGenreRank"));
		
		ControlledJob controlledJob4 = new ControlledJob(conf4);
		controlledJob4.setJob(job4);
		
		//chaining job3 to job4
		controlledJob4.addDependingJob(controlledJob3);
		jobControl.addJob(controlledJob4);

		/*******************************************************************************/
		Thread jobControlThread = new Thread(jobControl);
	    jobControlThread.start();

		while (!jobControl.allFinished()) {
//		    System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());  
//		    System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
//		    System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
//		    System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
//		    System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
		try {
		    Thread.sleep(20000);
		    } catch (Exception e) {
	
		    }
	
		  }
		
		System.exit(0);
		return(job1.waitForCompletion(true) ? 0 : 1);

	}
	
	public static void main(String args[]) throws Exception
	{
		int exitCode = ToolRunner.run(new MovieLensRankGenreDriver(), args);
		System.out.print("Control is HERE");
		System.exit(exitCode);
		
	}

}
