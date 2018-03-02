package com.github.skraina.movielens.topviewed;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MovieLensTopNMoviesMapper extends Mapper<LongWritable, Text, LongWritable, Text> 
{
	// This mapper simply emits count of ratings as key and movie name as values so that records are sorted
	// in decreasing order on count of ratings.
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		// Record is in the format (K,V) where K is count of total ratings and V is movie name.
		String line = value.toString();
		final String [] data = line.trim().split("\t");
		long MovieRating = Long.parseLong(data[0]);
		String MovieName = data[1];
		
		// Mapper keys will be sorted in decreasing order as the sorting comparator set for this
		// job is LongWritable.DecreasingComparator.
		context.write(new LongWritable(MovieRating), new Text(MovieName));
	}

}
