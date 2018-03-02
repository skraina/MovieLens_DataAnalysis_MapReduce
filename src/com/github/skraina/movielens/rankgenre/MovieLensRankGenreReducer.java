package com.github.skraina.movielens.rankgenre;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieLensRankGenreReducer extends Reducer<CompositeKeyWritableUID, Text, Text, Text> 
{
	MultipleOutputs<Text, Text> mos = null;
	int roCount = 0;
	public void setup(Context context)
	{
		mos = new MultipleOutputs<Text, Text>(context);
		roCount = 0;
	}
	
	public void reduce(CompositeKeyWritableUID CKey, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		Iterator<Text> first = values.iterator();
		String AgeOccp = first.next().toString(); // users.dat records come first so filtering out age group
		
		//StringBuffer movieIDRating = new StringBuffer();
		if(CKey.getjoinKey() == "Faulty Record")
		{
			for(Text val: values)
				mos.write("BadRecords", val, new Text());
		}
		else
		{
			//context.write(new Text(CKey.getjoinKey()), new Text(AgeOccp));

			if(AgeOccp.startsWith("AG")) // Filtering relevant (as per KPI) age groups' ratings records
			{
				Iterator<Text> iter = values.iterator();
				while(iter.hasNext())
				{
					//context.write(new Text(CKey.getjoinKey() + "::" + AgeOccp), iter.next());
					//context.write(new Text(CKey.getjoinKey()), iter.next());
					context.write(new Text(AgeOccp), iter.next()); //userID replaced by age group-occupation and value is movieID:rating by this userID
				}	

			}
		
/*			for(Text val: values)
			{
				movieIDRating.append(val.toString()).append("#");
			}
			if(AgeOccp.startsWith("AG"))
			{
				context.write(new Text(CKey.getjoinKey()), new Text(movieIDRating.toString()));
			}
*/			
		}

		
	}


}
