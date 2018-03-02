package com.github.skraina.movielens.topviewed;

import java.io.IOException;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class MovieLensTopViewCombiner extends Reducer<CompositeKeyWritableMID, Text, CompositeKeyWritableMID, Text> 
{
	public void reduce(CompositeKeyWritableMID CKey, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		int countRatings = 0;
		if(CKey.getjoinKey().equals("Faulty Record"))
		{
			for(Text val: values)
				context.write(CKey, val);
		}
		else
		{
			// Since CKey[movieID, 1] is different than CKey[movieID, 2] combiner(s) will treat them
			// differently (no combiner key grouping) and only aggregate ratings (count) 
			// from (movieID+2 [rating1, rating2,...] and will send as them as different keys to reducers. 
			// GroupingComparator only works for reducers so movieName and ratings will be combined in
			// the reduce phase only.
			if(CKey.getFileIndex() == 1) // Get movieName first and send it for shuffle and sort
			{
				context.write(CKey, values.iterator().next());
			}
			else
			{
				//Aggregate (count) only the number of ratings for each movieID and send for shuffle and sort.
				for(@SuppressWarnings("unused") Text val: values)
				{
					countRatings = countRatings + 1;
				}
				context.write(CKey, new Text(Integer.toString(countRatings)));
			}
		}
	}
}
