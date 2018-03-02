package com.github.skraina.movielens.toprated;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;


public class MovieLensTopNRatedMoviesMapper extends Mapper<LongWritable, Text, FloatWritable, Text> 
{
	// This mapper emits average of ratings as the key and count of ratings and movie names as values.
	// It simply sorts the records on average of ratings (highest first) using the custom class SortFloatWritableComparator.
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		final String [] data = line.trim().split("\t");
		
		float MovieRatingsAvg = Float.parseFloat(data[0]);
		long MovieRatingsCount = Long.parseLong(data[1]);
		String MovieName = data[2];
		
		context.write(new FloatWritable(MovieRatingsAvg), new Text(Long.toString(MovieRatingsCount) + "\t" + MovieName));
		
	}

}
