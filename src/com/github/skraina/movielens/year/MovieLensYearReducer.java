package com.github.skraina.movielens.year;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.Reducer;


public class MovieLensYearReducer extends Reducer<Text, Text, Text, Text> 
{
	MultipleOutputs<Text, Text> mos = null;
	
	public void setup(Context context)
	{
		mos = new MultipleOutputs<Text, Text>(context);
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
        mos.close();
	}

	public void reduce(Text key, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		// Reducer task appends each movie name and count all movies released in a particular year.
		int count = 0;
		StringBuilder MovieNames = new StringBuilder();
		
		// If key has been set by mapper as Duplicate Movie ID then this record goes into
		// BadRecords folder inside output directory.
		if(key.toString().equals("Duplicate Movie ID"))
		{
			for(Text val: values)
				mos.write("BadRecords", val, new Text());
		}
		else
		{
			Iterator<Text> iter = values.iterator();
			while(iter.hasNext())
			{
				count++;
				MovieNames.append(iter.next().toString());
				if(iter.hasNext())
					MovieNames.append("\n");

			}
			mos.write("ParsedRecords", key, new Text("Count of movies is: " + count + "\n" + MovieNames + "\n"));
		}
	}

}
