package com.github.skraina.movielens.rankgenre;

import java.io.IOException;
//import java.util.*;
//import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;


public class MovieLensRankGenreByAgeOccpCombiner extends Reducer<CompositeKeyWritableUID, Text, CompositeKeyWritableUID, Text>
{
	// This combiner reduces the amount of data to be transferred across network by computing the
	// count as well as the sum of ratings given by each Age Group and each occupation for each movie
	// Genre. It sends to reducers the CKey as key and [AG:Cnt:Sum] and [OC:Cnt:Sum] as values.

	public void reduce(CompositeKeyWritableUID CKey, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		if(CKey.getjoinKey().equals("Faulty Record"))
		{
			for(Text val: values)
				context.write(CKey, val);
		}
		else
		{

/* Since CKey[movieID, 3] has value as movie genre and CKey[movieID, 4] has values as AG:OC:Rating, 
 both keys are different. So combiner(s) will only aggregate ratings (count and sum) for each AgeGroup
 and Occupation Code and will send as different values to reducers for the same movieID.
 **GroupingComparator only works for reducers*** so keys will be combined in reduce phase.
*/
			if(CKey.getFileIndex() == 3)
			{
				context.write(CKey, values.iterator().next());
			}
			else
			{
				int RatingCount = 0;
				double RatingSum = 0.0;
				
				for(Text val : values)
				{
					RatingCount = RatingCount + 1;
					RatingSum = RatingSum + Double.parseDouble(val.toString());
				}
				//String movieID = CKey.getjoinKey().split("-")[0];
				String AGOC = CKey.getjoinKey().split("-")[1];
				//CKey.setjoinKey(movieID);
				context.write(CKey, new Text(AGOC + ":" + Integer.toString(RatingCount) + ":" + Double.toString(RatingSum)));
				//context.write(CKey, new Text(Integer.toString(RatingCount) + ":" + Double.toString(RatingSum)));

/*				TreeMap<String, Integer> AGOC_RatingCountMap = new TreeMap<String, Integer>();
				TreeMap<String, Double> AGOC_RatingSumMap = new TreeMap<String, Double>();

				for(Text val: values)
				{
					String AG = val.toString().split(":")[0];
					String OC = val.toString().split(":")[1];
					int newRating = Integer.parseInt(val.toString().split(":")[2]);
					
					Integer currAGRatingCount = AGOC_RatingCountMap.get(AG) == null ? 0 : AGOC_RatingCountMap.get(AG);
					Double currAGRatingSum = AGOC_RatingSumMap.get(AG) == null ? 0 : AGOC_RatingSumMap.get(AG);
					Integer currOCRatingCount = AGOC_RatingCountMap.get(OC) == null ? 0 : AGOC_RatingCountMap.get(OC);
					Double currOCRatingSum = AGOC_RatingSumMap.get(OC) == null ? 0 : AGOC_RatingSumMap.get(OC);
					
					AGOC_RatingCountMap.put(AG, currAGRatingCount + 1);
					AGOC_RatingCountMap.put(OC, currOCRatingCount + 1);
					AGOC_RatingSumMap.put(AG, currAGRatingSum + newRating);
					AGOC_RatingSumMap.put(OC, currOCRatingSum + newRating);
					
				}
*/				
/*				Iterator<Entry<String, Integer>> itr = AGOC_RatingCountMap.entrySet().iterator();
				while(itr.hasNext())
				{
					Map.Entry<String, Integer> AGOC_pair = (Map.Entry<String, Integer>) itr.next();
					String AGOCkey = AGOC_pair.getKey();
					Integer AGOC_RatingCount = AGOC_pair.getValue();
					//Integer AGOC_RatingCount = AGOC_RatingCountMap.get(AGOCkey);
					Double AGOC_RatingSum = AGOC_RatingSumMap.get(AGOCkey);
					context.write(CKey, new Text(AGOCkey + ":" + Integer.toString(AGOC_RatingCount) + ":" + Double.toString(AGOC_RatingSum)));
					
				}
*/				
			}

		}

	}
}
	


