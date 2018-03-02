package com.github.skraina.movielens.rankgenre;

import java.io.IOException;

//import java.util.Iterator;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;


public class MovieLensRankGenreFinalCombiner extends Reducer<Text, Text, Text, Text>
{
	// This combiner reduces the amount of data to be transferred across network by computing the
	// count as well as the sum of ratings for each movie name. It sends to reducers only CKey as key
	// and count and sum of ratings as values.

	public void reduce(Text key, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		int GenreRatingCount = 0;
		double GenreRatingSum = 0;

		for(Text val: values)
		{
			Integer GRC = Integer.parseInt(val.toString().split(":")[0]);
			Double GRS = Double.parseDouble(val.toString().split(":")[1]);
			GenreRatingCount = GenreRatingCount + GRC;
			GenreRatingSum = GenreRatingSum + GRS;
		}
		context.write(key, new Text(Integer.toString(GenreRatingCount) + "\t" + Double.toString(GenreRatingSum)));

	}

}
