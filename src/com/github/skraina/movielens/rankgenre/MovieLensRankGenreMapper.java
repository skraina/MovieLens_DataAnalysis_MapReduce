package com.github.skraina.movielens.rankgenre;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MovieLensRankGenreMapper extends Mapper<LongWritable, Text, CompositeKeyWritableUID, Text> 
{
	String inputFileName;
	CompositeKeyWritableUID CKey = new CompositeKeyWritableUID();
	int FileIndex = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException
	{
		// Get file index for each dataset: ratings = 1 and users = 2
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
/*		inputFileName = fileSplit.getPath().getName();
		if(inputFileName.equals("ratings.dat"))
		{
			FileIndex = 2;
		}
		else if(inputFileName.equals("users.dat"))
		{
			FileIndex = 1;
		}
		else return;
*/		FileIndex = context.getConfiguration().getInt(fileSplit.getPath().getName(),0);
		//FileIndex = Integer.parseInt(context.getConfiguration().get(fileSplit.getPath().getName()));
	}

/* This map function reads ratings.dat and users.dat. It emits userID as CKey and movieID + ratings from
 * ratings.dat and AgeGroup + Occupation code from users.dat */
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
//		Input file is ratings.dat
//		UID::MID::Rating::Time
//		1::1193::5::978300760		
//		Input file is users.data
//		UID::Gender::AgeCode::OccupationCode::Zip		
//		1::F::1::10::48067
		
		final String[] data = line.trim().split("::");
		
		if(FileIndex == 2) // ratings.dat
		{
			// data[0] = UID
			// data[1] = MID
			// data[2] = Rating
			
			if(data.length == 4)
			{
				String userID = data[0];
				String movieID = data[1];
				String rating = data[2];
				CKey.setjoinKey(userID);
				CKey.setFileIndex(FileIndex);
				context.write(CKey, new Text(movieID + ":" + rating));
			}
			else
			{
				CKey.setjoinKey("Faulty Record");
				CKey.setFileIndex(FileIndex);
				context.write(CKey, value);
			}
		}
		else if(FileIndex == 1) // users.dat
		{
			// data[0] = UID
			// data[1] = Gender
			// data[2] = AgeCode
			// data[3] = Occupation
			// data[4] = Zip
			
			if(data.length == 5)
			{
				if(data[2].equals("1")) // under 18 age group not required
					return;
				else
				{
					String userID = data[0];
					String OccpCode = "OC" + data[3];
					String AgeGroup = "";
					if(data[2].equals("18") || data[2].equals("25"))
					{
						AgeGroup = "AG1";
					}
					else if(data[2].equals("35") || data[2].equals("45"))
					{
						AgeGroup = "AG2";
					}
						else AgeGroup = "AG3";
					CKey.setjoinKey(userID);
					CKey.setFileIndex(FileIndex);
					context.write(CKey, new Text(AgeGroup + ":" + OccpCode));
				}
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
