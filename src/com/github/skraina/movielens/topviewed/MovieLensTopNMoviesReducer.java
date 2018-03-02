package com.github.skraina.movielens.topviewed;

import java.io.IOException;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieLensTopNMoviesReducer extends Reducer<LongWritable, Text, LongWritable, Text> 
{
/* This reducer combines movie names having same "count" of ratings and emits movie names associated
 * with top N (default is 10) distinct ratings sorted from highest rating count to lowest.
 * So count of movie names can be > N */
	
	private static int topNCount = 0;
	public void reduce(LongWritable key, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		int topN = context.getConfiguration().getInt("topNMovies", 10);
		if(topNCount < topN)
		{
			for(Text val : values)
			{
				// Key emitted is simply a serial number and the value consists of total rating
				// and all movie names having this rating.
				context.write(new LongWritable(topNCount + 1), new Text(key.toString() + "\t" + val.toString()));
				topNCount = topNCount + 1;
			}
		}
	}

}
