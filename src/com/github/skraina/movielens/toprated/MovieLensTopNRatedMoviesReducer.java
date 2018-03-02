package com.github.skraina.movielens.toprated;

import java.io.IOException;
//import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieLensTopNRatedMoviesReducer extends Reducer<FloatWritable, Text, LongWritable, Text> 
{
/* This reducer combines movie names having same "average" of ratings and emits movie names and their count
 * of ratings associated with top N (default is 20) distinct averages sorted from highest average to lowest.
* So count of movie names can be > N */

	private static int topNRatedCount = 0;
	public void reduce(FloatWritable key, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		int topNRated = context.getConfiguration().getInt("topNRatedMovies", 20);
		//StringBuffer movieSameRatings = new StringBuffer();
		if(topNRatedCount < topNRated)
		{
			for(Text val : values)
			{
				String Count_movieName[] = val.toString().split("\t");
				context.write(new LongWritable(topNRatedCount + 1), new Text(key.toString() + "\t" + Count_movieName[0].toString() + "\t" + Count_movieName[1].toString()));
				topNRatedCount = topNRatedCount + 1;
			}
		}
/*		for(Text val : values)
		{
			movieSameRatings.append(val.toString()).append("#");
		}
		
		context.write(key, new Text(movieSameRatings.toString() + "::" + Integer.toString(topN)));
*/	}

}
