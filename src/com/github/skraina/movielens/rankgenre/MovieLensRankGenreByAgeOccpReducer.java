package com.github.skraina.movielens.rankgenre;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

//public class MovieLensRankGenreByAgeOccpReducer extends Reducer<CompositeKeyWritableUID, Text, CompositeKeyWritableUID, Text>
public class MovieLensRankGenreByAgeOccpReducer extends Reducer<CompositeKeyWritableUID, Text, Text, Text> 
{
	MultipleOutputs<Text, Text> mos = null;
	int roCount = 0;
	public void setup(Context context)
	{
		mos = new MultipleOutputs<Text, Text>(context);
		roCount = 0;
	}
	
	//public static HashMap<String, HashMap<String, Double>> GenreRatingsTable = new HashMap<String, HashMap<String, Double>>();

	public void reduce(CompositeKeyWritableUID CKey, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		String MovieGenreGroup = values.iterator().next().toString(); // movies.data comes first in order so extracting each movie genre.
		//StringBuilder strb = new StringBuilder();
		if(CKey.getjoinKey() == "Faulty Record")
		{
			for(Text val: values)
				mos.write("BadRecords", val, new Text());
		}
		else
		{
/*			for(Text val : values)
			{
				strb.append(val.toString()).append(" # ");
			}
			context.write(new Text(CKey.getjoinKey().split("-")[0]), new Text(strb.toString()));
*/			//context.write(CKey, new Text(strb.toString()));
			// Filtering relevant age groups' ratings records
			if(!MovieGenreGroup.startsWith("AG") && !MovieGenreGroup.startsWith("OC")) // Movies not having genre are not considered in analysis.
			//if(!MovieGenreGroup.matches("[0-9]")) // Movies not having genre are not considered in analysis.
			{
				Iterator<Text> iter = values.iterator();
				while(iter.hasNext())
				{
					//Aggregated AGs and OCs with their count and sum of ratings for Genre Group (each movie)
					context.write(iter.next(), new Text(CKey.getjoinKey().split("-")[0] + ":" + MovieGenreGroup));
					//context.write(new Text(CKey.getAGOC() + ":" + iter.next()), new Text(CKey.getjoinKey()));
					//context.write(iter.next(), new Text(CKey.getjoinKey()));
				}	

			}
		
		}
	}
	

}
