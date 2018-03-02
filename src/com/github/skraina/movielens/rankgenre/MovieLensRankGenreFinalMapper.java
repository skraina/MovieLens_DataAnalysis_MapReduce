package com.github.skraina.movielens.rankgenre;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MovieLensRankGenreFinalMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		// Each record is of the format as
		//AgeGroup : Count of Rating : Sum of Rating \t GenreGroup <AG1:1:3.0 \t >
		// OR
		// OC : Count of Rating : Sum of Rating \t GenreGroup <OC16:1:3.0>
		
		final String[] data = line.trim().split("\t");
		//data[0] = AG:1:3.0    OR     OC16:1:3.0
		//data[1] = Action|Adventure|Thriller
		
		String AGOC = data[0].split(":")[0];  // First part is either AG or OC
		String GenreRatingCount = data[0].split(":")[1];  // Second part is count of rating
		String GenreRatingSum = data[0].split(":")[2];  // Third part is sum of rating
		// Just for the sake of testing movieID with correct computation on ratings.
		// remove below two statements after testing is over
		// Also correct the reducer value by removing CKey.getJoinKey() from MLRGByAgeOccpReducer.java
		//String movieID = data[1].split(":")[0];
		String [] genres = data[1].split(":")[1].split("\\|");
		
		//String [] genres = data[1].split("\\|");
		
		for(String genre : genres)
		{
			context.write(new Text(AGOC + "::" + genre), new Text(GenreRatingCount + ":" + GenreRatingSum));
		}
		
		//context.write(new Text(AG), new Text(data[1] + "::" + rating));
		//context.write(new Text(OC), new Text(data[1] + "::" + rating));
	}
	
}
