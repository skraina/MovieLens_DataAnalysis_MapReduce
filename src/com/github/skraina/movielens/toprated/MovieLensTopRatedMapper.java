package com.github.skraina.movielens.toprated;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MovieLensTopRatedMapper extends Mapper<LongWritable, Text, CompositeKeyWritableMID, Text> 
{
	CompositeKeyWritableMID CKey = new CompositeKeyWritableMID();
	int FileIndex = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		// Get file index for each dataset: movies.dat = 1, ratings.dat = 2
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		FileIndex = context.getConfiguration().getInt(fileSplit.getPath().getName(),0);
	}

/* This map reads movies.dat and ratings.dat. It emits movieID+FileIndex as the key and movieName and 
	 * ratings as values. An aggregation on the count and sum of ratings is done in combiners. */

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
//		Input file is movies.dat		
//		1::Toy Story (1995)::Animation|Children's|Comedy
//		Input file is ratings.dat
//		1::1193::5::978300760		
//		Top viewed movies are those which have more NUMBER of ratings from users.
//		The datasets movies.dat and ratings.dat are joined ON movieID and number of ratings is computed.
		
		final String[] data = line.trim().split("::");
		
		if(FileIndex == 1) // File is movies.dat
		{
			// data[0] = 1
			// data[1] = Toy Story (1995)
			// data[2] = Animation|Children's|Comedy
			
			if(data.length == 3)
			{
				String movieID = data[0];
				//String movie[] = data[1].split("(?<=\\()(?=\\d)|(?<=\\d)(?=\\))");
				//String year = movie[1];
				String movieName = data[1];
				CKey.setjoinKey(movieID);
				CKey.setFileIndex(FileIndex);
				context.write(CKey, new Text(movieName));
			}
			else
			{
				CKey.setjoinKey("Faulty Record");
				CKey.setFileIndex(FileIndex);
				context.write(CKey, value);
			}
		}
		else // File is ratings.dat
		{
			// data[0] = 1 userID
			// data[1] = 1193 movieID
			// data[2] = 5 rating
			// data[3] = 978300760 timestamp in seconds since epoch
			
			if(data.length == 4)
			{
				String movieID = data[1];
				String rating = data[2];
				CKey.setjoinKey(movieID);
				CKey.setFileIndex(FileIndex);
				context.write(CKey, new Text(rating));
			}
			else
			{
				CKey.setjoinKey("Faulty Record");
				CKey.setFileIndex(FileIndex);
				context.write(CKey, value);
			}

		}
		//context.write(key,  new Text(line));
	}
}
