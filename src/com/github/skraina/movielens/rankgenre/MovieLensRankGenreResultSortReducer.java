package com.github.skraina.movielens.rankgenre;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

//public class MovieLensRankGenreResultSortReducer extends Reducer<CompositeKeyWritableAGOC, Text, CompositeKeyWritableAGOC, Text>
public class MovieLensRankGenreResultSortReducer extends Reducer<CompositeKeyWritableAGOC, Text, Text, Text>
{
	/* This reducer combines movie names having same "average" of ratings and emits movie names and their count
	 * of ratings associated with top N (default is 20) distinct averages sorted from highest average to lowest.
	* So count of movie names can be > N. */ 

	public void reduce(CompositeKeyWritableAGOC CKey, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		StringBuilder Genre_Rating = new StringBuilder();
		Iterator<Text> iter = values.iterator();
		while(iter.hasNext())
		{
			Genre_Rating.append(iter.next().toString());
			if(iter.hasNext())
			{
				Genre_Rating.append(" < ");
			}
			
		}
/*		for(Text val : values)
		{
			
			//Genre_Rating.append(val.toString()).append(" < ");
			//String AG_OC = val.toString().split("::")[0];
			//Genre.append(val.toString().split("::")[1]);
			//context.write(new Text(CKey.getjoinKey()), new Text(val.toString()));
			
		}
*/		
		context.write(new Text(CKey.getjoinKey()), new Text("\n" + Genre_Rating.toString() + "\n"));
		//context.write(CKey, new Text(Genre_Rating.toString()));

	}

}
	
	

