package com.github.skraina.movielens.toprated;

import java.io.IOException;
import java.text.DecimalFormat;

//import java.util.*;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;


public class MovieLensTopRatedReducer extends Reducer<CompositeKeyWritableMID, Text, Text, Text> 
{
	MultipleOutputs<Text, Text> mos = null;
	int roCount = 0;
	public void setup(Context context)
	{
		mos = new MultipleOutputs<Text, Text>(context);
		roCount = 0;
	}
	
	//public static TreeMap<String, Double> MovieMap = new TreeMap<String, Double>();
	DecimalFormat format = new DecimalFormat("##.00");

	// Values (count and sum of ratings) for the key "movieID" can come from different mappers/combiners so a
	// final count and final sum must be computed, before computing the rating average, by adding all the 
	// counts and sums from different combiners.
	// This reducer emits average of ratings as key only for movies rated by more than 40 users, 
	// and count of ratings and movie name as values. A second mapper then sorts on average of ratings.

	public void reduce(CompositeKeyWritableMID CKey, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		int movieRatingsFinalCount = 0;
		double movieRatingsFinalSum = 0;
		//int count = 0;
		String movieName = values.iterator().next().toString();
//		int countRatings = 0;
//		double sumRatings = 0;
		float movieRatingsAvg = 0;
		//StringBuffer movieRating = new StringBuffer();
		if(CKey.getjoinKey().equals("Faulty Record"))
		{
			for(Text val: values)
				mos.write("BadRecords", CKey.getjoinKey(), val);
		}
		else
		{
			for(Text val: values)
			{
				String Count_Sum[] = val.toString().split("\t");
				
				movieRatingsFinalCount = movieRatingsFinalCount + Integer.parseInt(Count_Sum[0].toString());
				movieRatingsFinalSum = movieRatingsFinalSum + Double.parseDouble(Count_Sum[1].toString());
//				countRatings++;
//				sumRatings = sumRatings + Integer.parseInt(val.toString());
					//movieRating.append(val.toString()).append("#");
				
			}
			//if(countRatings != 0 && countRatings >= 40)
			if(movieRatingsFinalCount >= 40)	
			{
				movieRatingsAvg = Float.parseFloat(format.format(movieRatingsFinalSum / movieRatingsFinalCount));
//				MovieMap.put(new String(movieName + "\t\t" + key.toString() + "\t" +
//				sumRatings + "\t" + countRatings), avgRatings);
				//MovieMap.put(new String(movieName + "\t" + countRatings), avgRatings);
				context.write(new Text(Float.toString(movieRatingsAvg)), new Text(movieRatingsFinalCount + "\t" + movieName));
			//context.write(new Text(CKey.getjoinKey()), new Text(movieRating.toString()));
			}
			
			
		}
	}

/*	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		LinkedHashMap<String, Double> SortedMovieMap = new LinkedHashMap<String, Double>();
		SortedMovieMap = sortMap(MovieMap);
		mos.write("ParsedRecords", 
				new Text("\n Top_20_most_rated_movies by Average Rating in descending order"), new Text(" \n"));
		mos.write("ParsedRecords", new Text("MOVIE NAME (YEAR) "
				+ "CountRating"), new Text("AVG RATING"));
		for(Map.Entry<String, Double> entry : SortedMovieMap.entrySet())
		{
			mos.write("ParsedRecords", new Text(entry.getKey()), new Text(entry.getValue().toString()));
		}
		mos.close();
	}
	
	public LinkedHashMap<String, Double> sortMap(Map<String, Double> MvMap)
	{
		LinkedHashMap<String, Double> HMap = new LinkedHashMap<String, Double>();
		int countMovies = 0;
		List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(MvMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>()
				{
					public int compare(Map.Entry<String, Double> r1, Map.Entry<String, Double> r2)
					{
						return r2.getValue().compareTo(r1.getValue());
					}
				});
		
		for(Map.Entry<String, Double> entry : list)
		{
			if(countMovies++ > 20)
				break;
			HMap.put(entry.getKey(), entry.getValue());
		}
		return HMap;
	}
*/	
	


}
