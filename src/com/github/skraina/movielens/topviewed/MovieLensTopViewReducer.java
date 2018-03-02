package com.github.skraina.movielens.topviewed;

import java.io.IOException;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;


public class MovieLensTopViewReducer extends Reducer<CompositeKeyWritableMID, Text, Text, Text>
{
	MultipleOutputs<Text, Text> mos = null;
	int roCount = 0;
	public void setup(Context context)
	{
		mos = new MultipleOutputs<Text, Text>(context);
		roCount = 0;
	}

	// Values (count of ratings) for the key "movieID" can come from different mappers/combiners so a
	// final count must be computed by adding all the counts from different combiners.
	// This reducer emits count of ratings as key and movie name as value.
	// A second mapper then sorts on count of ratings.
	
	public void reduce(CompositeKeyWritableMID CKey, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		int movieRatingsFinalCount = 0;
		// Movie name appears as the first value in the list of values for each movieID.
		String movieName = values.iterator().next().toString();
		if(CKey.getjoinKey().equals("Faulty Record"))
		{
			for(Text val: values)
				mos.write("BadRecords", CKey.getjoinKey(), val);
		}
		else
		{
			for(Text val: values)
			{
				movieRatingsFinalCount = movieRatingsFinalCount + Integer.parseInt(val.toString());
			}
			context.write(new Text(Integer.toString(movieRatingsFinalCount)), new Text(movieName));
		}
	}
}
