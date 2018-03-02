package com.github.skraina.movielens.topviewed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
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


public class MovieLensTopViewDriver extends Configured implements Tool
{
	public int run(String args[]) throws Exception
	{
		JobControl jobControl = new JobControl("MovieLensTopView");
		Configuration conf1 = getConf();
		// File indices must be set before passing conf1 to job1.
		conf1.setInt("movies.dat", 1);
		conf1.setInt("ratings.dat", 2);
		
		// job1 performs the following in this order
		// 1) Reads two files/datasets movies.dat and ratings.dat
		// 2) Joins the above two datasets and groups all the ratings by movieIDs and 
		// aggregate count of ratings for each movie.
		// 3) Emit count of ratings as key and movie name as value for job2.
		Job job1 = Job.getInstance(conf1);
		job1.setJobName("Movie Lens DA Top Viewed Movies - Ratings :: Movie Names");
		job1.setJarByClass(MovieLensTopViewDriver.class);
		job1.setNumReduceTasks(1);
		job1.setMapperClass(MovieLensTopViewMapper.class);
		job1.setCombinerClass(MovieLensTopViewCombiner.class);
		job1.setReducerClass(MovieLensTopViewReducer.class);
		job1.setSortComparatorClass(SortingComparatorMID.class);
		job1.setPartitionerClass(PartitionerMID.class);
		job1.setGroupingComparatorClass(GroupingComparatorMID.class);		
	
		job1.setMapOutputKeyClass(CompositeKeyWritableMID.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		MultipleOutputs.addNamedOutput(job1, "ParsedRecords", TextOutputFormat.class , Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job1, "BadRecords", TextOutputFormat.class , Text.class, Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0] + "/movies.dat"));
		FileInputFormat.addInputPath(job1, new Path(args[0] + "/ratings.dat"));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/RatingsMovieNames"));

		ControlledJob controlledJob1 = new ControlledJob(conf1);
		controlledJob1.setJob(job1);
		
		jobControl.addJob(controlledJob1);
		
/************************************************************************************************/
		// job2 reads job1 output and simply groups all movie names having same total ratings
		// and emits total ratings as key and group(s) of movie names as list of value(s)
		// in decreasing order of ratings. By default job2 produces top (N=10) most viewed
		// movies but value of N can be given at command-line as an argument.
		Configuration conf2 = getConf();
		Job job2 = Job.getInstance(conf2);
		job2.setJobName("Movie Lens DA Top N Viewed Movies");
		job2.setJarByClass(MovieLensTopViewDriver.class);
		job2.setNumReduceTasks(1);
		job2.setMapperClass(MovieLensTopNMoviesMapper.class);
		job2.setReducerClass(MovieLensTopNMoviesReducer.class);
		job2.setSortComparatorClass(LongWritable.DecreasingComparator.class); // Sort from highest rating
		
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job2, new Path(args[1] + "/RatingsMovieNames"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/TopNViewedMovies"));
		
		if(args.length == 3)
		{
			// Get value of N (top number of most viewed movies) from command-line.
			job2.getConfiguration().setInt("topNMovies", Integer.parseInt(args[2])); 
		}
		
		ControlledJob controlledJob2 = new ControlledJob(conf2);
		controlledJob2.setJob(job2);
		
		// Create pipeline job1 -> job2
		controlledJob2.addDependingJob(controlledJob1);
		jobControl.addJob(controlledJob2);
		
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
		return(job1.waitForCompletion(true) ? 0: 1);
	}
	
	public static void main(String args[]) throws Exception
	{
		int exitCode = ToolRunner.run(new MovieLensTopViewDriver(), args);
		System.out.print("Control is HERE");
		System.exit(exitCode);
		
	}


}
