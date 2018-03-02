package com.github.skraina.movielens.topviewed;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class MovieLensTopViewMapper extends Mapper<LongWritable, Text, CompositeKeyWritableMID, Text> 
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
 * ratings as values. An aggregation on the count of ratings is done in combiners. */
	
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
			
			if(data.length != 3 || (data[0].isEmpty() || data[1].isEmpty() || data[2].isEmpty()))
			{
				// If movie records does not contain all the three fields then records directly go
				// to Bad Records folder inside output directory in HDFS
				CKey.setjoinKey("Faulty Record");
				CKey.setFileIndex(FileIndex);
				context.write(CKey, value);
			}
			else
			{
				// MovieID + FileIndex is a key so the reducer will get values for each key in the format
				// (key, [movieName, rating1, rating2, ...]) since sorting comparator keeps
				// value of key having file index 1 ahead of values of key having file index 2.
				// Combiner gets (movieID+1, movieName) and (movieID+2, [rating1, ratings2, ...])
				String movieID = data[0];
				String movieName = data[1]; // Movie name with year
				CKey.setjoinKey(movieID);
				CKey.setFileIndex(FileIndex);
				context.write(CKey, new Text(movieName));
			}
		}
		else  // File is ratings.dat
		{
			// data[0] = 1 userID
			// data[1] = 1193 movieID
			// data[2] = 5 rating
			// data[3] = 978300760 timestamp in seconds since epoch
			
			if(data.length != 4 || (data[0].isEmpty() || data[1].isEmpty() || data[2].isEmpty()) || data[3].isEmpty())
			{
				// If movie records does not contain all the four fields then records directly go
				// to Bad Records folder inside output directory in HDFS
				CKey.setjoinKey("Faulty Record");
				CKey.setFileIndex(FileIndex);
				context.write(CKey, value);
			}
			else
			{
				// movieID+FileIndex acts as a key and all the ratings given to this movie by each user
				// gets combined in a list of values for combiner/reducer.
				String movieID = data[1];
				String rating = data[2];
				CKey.setjoinKey(movieID);
				CKey.setFileIndex(FileIndex);
				context.write(CKey, new Text(rating));
			}

		}
		
	}

}
