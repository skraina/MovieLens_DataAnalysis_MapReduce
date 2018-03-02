package com.github.skraina.movielens.rankgenre;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MovieLensRankGenreResultSortMapper extends Mapper<LongWritable, Text, CompositeKeyWritableAGOC, Text> 
{
	CompositeKeyWritableAGOC CKey = new CompositeKeyWritableAGOC();
	// This mapper emits average of ratings as the key and (Age/Occupation group + movie genres) as values.
	// It simply sorts the records on average of ratings (highest first) using the custom class SortFloatWritableComparator.
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		final String [] data = line.trim().split("\t");
		float GenreRatingsAvg = Float.parseFloat(data[1]);
		String AGOC = data[0].split("::")[0];
		String Genre = data[0].split("::")[1];
		
		switch(AGOC)
        {
            case "AG1":
            	CKey.setjoinKey("Age Group 18-34");
                break;
            case "AG2":
            	CKey.setjoinKey("Age Group 35-49");
                break;
            case "AG3":
            	CKey.setjoinKey("Age Group 50+");
                break;
            case "OC0":
            	CKey.setjoinKey("other");
            	break;
            case "OC1":
            	CKey.setjoinKey("academic/educator");
                break;
            case "OC2":
            	CKey.setjoinKey("artist");
                break;
            case "OC3":
            	CKey.setjoinKey("clerical/admin");
                break;
            case "OC4":
            	CKey.setjoinKey("college/grad student");
                break;
            case "OC5":
            	CKey.setjoinKey("customer service");
                break;
            case "OC6":
            	CKey.setjoinKey("doctor/health care");
                break;
            case "OC7":
            	CKey.setjoinKey("executive/managerial");
                break;
            case "OC8":
            	CKey.setjoinKey("farmer");
                break;
            case "OC9":
            	CKey.setjoinKey("homemaker");
                break;
            case "OC10":
            	CKey.setjoinKey("K-12 student");
                break;
            case "OC11":
            	CKey.setjoinKey("lawyer");
                break;
            case "OC12":
            	CKey.setjoinKey("programmer");
                break;
            case "OC13":
            	CKey.setjoinKey("retired");
                break;
            case "OC14":
            	CKey.setjoinKey("sales/marketing");
                break;
            case "OC15":
            	CKey.setjoinKey("scientist");
                break;
            case "OC16":
            	CKey.setjoinKey("self-employed");
                break;
            case "OC17":
            	CKey.setjoinKey("technician/engineer");
                break;
            case "OC18":
            	CKey.setjoinKey("tradesman/craftsman");
                break;
            case "OC19":
            	CKey.setjoinKey("unemployed");
                break;
            case "OC20":
            	CKey.setjoinKey("writer");
                break;
        }
		
		//CKey.setjoinKey(AGOC);
		CKey.setRatingAvg(GenreRatingsAvg);
		
		//context.write(new FloatWritable(GenreRatingsAvg), new Text(AGOC_Genre));
		context.write(CKey, new Text(Genre + ":" + Float.toString(GenreRatingsAvg)));
	}


}
