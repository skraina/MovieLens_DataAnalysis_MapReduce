package com.github.skraina.movielens.rankgenre;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MovieLensRankGenreByAgeOccpMapper extends Mapper<LongWritable, Text, CompositeKeyWritableUID, Text> 
{
	String inputFileName;
	CompositeKeyWritableUID CKey = new CompositeKeyWritableUID();
	int FileIndex = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		// Get file index for movie dataset: movies = 3 and Job1 output: Age-Occupation-Rating data = 4
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
/*		inputFileName = fileSplit.getPath().getName();
		if(inputFileName.equals("movies.dat"))
		{
			FileIndex = 3;
		}
		else //if(inputFileName.equals("users.dat"))
		{
			FileIndex = 4;
		}
		//else return;
*/		
		FileIndex = context.getConfiguration().getInt(fileSplit.getPath().getName(),4);
		//FileIndex = Integer.parseInt(context.getConfiguration().get(fileSplit.getPath().getName()));
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
//		Input file is movies.dat
//		MID::Movie name::Genre
//		1::Toy Story (1995)::Animation|Children's|Comedy		
//		Input file is Job1 output data
//		AgeGroup-OccupationCode \t MID:Rating
//		AG1-17	1310:3
		
		
		if(FileIndex == 3) // movies.dat
		{
			// data[0] = MID
			// data[1] = Movie Name
			// data[2] = Genre
			final String[] data = line.trim().split("::");
			
			if(data.length == 3)
			{
				String movieID = data[0];
				String genre = data[2];
				CKey.setjoinKey(movieID);
				CKey.setFileIndex(FileIndex);
				//CKey.setAGOC("AGOC");
				context.write(CKey, new Text(genre));
			}
			else
			{
				CKey.setjoinKey("Faulty Record");
				CKey.setFileIndex(FileIndex);
				context.write(CKey, value);
			}
		}
		else // Job1 output data
		{
			// data[0] = AgeGroup:Occupation
			// data[1] = MID:rating
			final String[] data = line.trim().split("\t");
			
			if(data.length == 2)
			{
				String AgeGroup = data[0].split(":")[0];
				String OccpCode = data[0].split(":")[1];
				String movieID = data[1].split(":")[0];
				String rating = data[1].split(":")[1];
				CKey.setjoinKey(movieID + "-" + AgeGroup);
				CKey.setFileIndex(FileIndex);
				context.write(CKey, new Text(rating));
				CKey.setjoinKey(movieID + "-" + OccpCode);
				context.write(CKey, new Text(rating));
				//CKey.setAGOC(AgeGroup);
				//context.write(CKey, new Text(rating));
				//CKey.setAGOC(OccpCode);
				//context.write(CKey, new Text(rating));
				//context.write(CKey, new Text(AgeGroup + ":" + OccpCode + ":" + rating));
			}
			else
			{
				CKey.setjoinKey("Faulty Record");
				CKey.setFileIndex(FileIndex);
				context.write(CKey, value);
			}

		}
	}


}
