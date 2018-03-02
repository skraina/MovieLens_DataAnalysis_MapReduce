package com.github.skraina.movielens.toprated;

import java.io.IOException;

//import java.util.Iterator;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;


public class MovieLensTopRatedCombiner extends Reducer<CompositeKeyWritableMID, Text, CompositeKeyWritableMID, Text> 
{
	// This combiner reduces the amount of data to be transferred across network by computing the
	// count as well as the sum of ratings for each movie name. It sends to reducers only CKey as key
	// and count and sum of ratings as values.
	
	public void reduce(CompositeKeyWritableMID CKey, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		int countRatings = 0;
		double sumRatings = 0;
		//String movieName = "";
		//StringBuffer movieRating = new StringBuffer();
		if(CKey.getjoinKey().equals("Faulty Record"))
		{
			for(Text val: values)
				context.write(CKey, val);
		}
		else
		{
			// Since CKey[movieID, 1] has value as movieName and CKey[movieID, 2] has value as ratings, 
			// both keys are different. So combiner(s) will aggregate ratings (count and sum) and will send as
			// different keys to reducers. **GroupingComparator only works for reducers*** so keys will be
			// combined in reduce phase.

			if(CKey.getFileIndex() == 1)
			{
				context.write(CKey, values.iterator().next());
			}
			else
			{
				for(Text val: values)
				{
					countRatings = countRatings + 1;
					sumRatings = sumRatings + Integer.parseInt(val.toString());
				}
				context.write(CKey, new Text(Integer.toString(countRatings) + "\t" + Double.toString(sumRatings)));
			}
/*			for(Text val: values)
			{
				if(val.toString().matches(".*[()].*"))
				{
					movieName = val.toString();
				}
				else
				{
					// Count the number of ratings, higher the number, most viewed is the movie
					countRatings++;
					//movieRating.append(val.toString()).append("#");
				}
				//movieName = movieName.concat(val.toString());
				//movieRating.append(val.toString()).append("#");
				
			}
			//context.write(new Text(CKey.getjoinKey() + "::" + CKey.getFileIndex()), new Text(movieRating.toString()));
			context.write(CKey, new Text(movieRating.toString()));
*/		}
	}
}
