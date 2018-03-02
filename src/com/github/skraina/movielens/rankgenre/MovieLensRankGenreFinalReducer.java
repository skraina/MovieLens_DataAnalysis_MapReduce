package com.github.skraina.movielens.rankgenre;

import java.io.IOException;
//import java.util.*;
import java.text.DecimalFormat;
//import java.util.HashMap;

import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


public class MovieLensRankGenreFinalReducer extends Reducer<Text, Text, Text, Text>
{

	//public static HashMap<String, TreeMap<String, Double>> GenreRatingsTable = new HashMap<String, TreeMap<String, Double>>();	
	DecimalFormat format = new DecimalFormat("##.00");
	
	//public void reduce(Text key, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	public void reduce(Text key, Iterable<Text> values, final Context context) throws IOException, InterruptedException
	{
		
		int GenreRatingsFinalCount = 0;
		double GenreRatingsFinalSum = 0;
		double GenreRatingsAvg = 0;
		
		for(Text val: values)
		{
			//String Count_Sum[] = val.toString().split("\t");
			String GenreRatingsCount = val.toString().split("\t")[0];
			String GenreRatingsSum = val.toString().split("\t")[1];
			
			GenreRatingsFinalCount = GenreRatingsFinalCount + Integer.parseInt(GenreRatingsCount);
			GenreRatingsFinalSum = GenreRatingsFinalSum + Double.parseDouble(GenreRatingsSum);
		}
		
		GenreRatingsAvg = Double.parseDouble(format.format(GenreRatingsFinalSum / GenreRatingsFinalCount));
		//GenreRatingsAvg = GenreRatingsFinalSum / GenreRatingsFinalCount;
		//context.write(new Text(Float.toString(GenreRatingsAvg)), new Text(Integer.toString(GenreRatingsFinalCount) + "\t" + key));
		context.write(key, new Text(Double.toString(GenreRatingsAvg)));

		
		
/*		int genreCnt = 0;
		HashMap<String, String> AGOCname = new HashMap<String, String>();
        TreeMap<String, Double> GenreMap = new TreeMap<String, Double>();
        TreeMap<String, Integer> GenreCnt = new TreeMap<String, Integer>();
        TreeMap<String, Double> GenreAvgRating = new TreeMap<String, Double>();
        LinkedHashMap<String, Double> SortedGenreAvgRating = new LinkedHashMap<String, Double>();
        
        AGOCname.put("AG1", "Age Group: 18-35");
        AGOCname.put("AG2", "Age Group: 36-50");
        AGOCname.put("AG3", "Age Group: 50+");
        AGOCname.put("OC0", "Other");
        AGOCname.put("OC1", "academic/educator");
        AGOCname.put("OC2", "artist");
        AGOCname.put("OC3", "clerical/admin");
        AGOCname.put("OC4", "colleg/grad student");
        AGOCname.put("OC5", "customer service");
        AGOCname.put("OC6", "doctor/health care");
        AGOCname.put("OC7", "executive/managerial");
        AGOCname.put("OC8", "farmer");
        AGOCname.put("OC9", "homemaker");
        AGOCname.put("OC10", "K-12 student");
        AGOCname.put("OC11", "lawyer");
        AGOCname.put("OC12", "programmer");
        AGOCname.put("OC13", "retired");
        AGOCname.put("OC14", "sales/marketing");
        AGOCname.put("OC15", "scientist");
        AGOCname.put("OC16", "self-employed");
        AGOCname.put("OC17", "technician/engineer");
        AGOCname.put("OC18", "tradesman/craftsman");
        AGOCname.put("OC19", "unemployed");
        AGOCname.put("OC20", "writer");
        
        String AGOC = AGOCname.get(key.toString());
*/        
/*        for(Text GenresRating:values)
        {
        	genreCnt = genreCnt + 1;
        	String[] GRdata = GenresRating.toString().split("::");
        	String[] Genres = GRdata[0].split("\\|");
            Double newRating = Double.parseDouble(GRdata[1]);
            
            for(String genre:Genres)
            {
            	Double currRating = GenreMap.get(genre) == null ? 0.0 : GenreMap.get(genre);
            	GenreMap.put(genre, currRating + newRating);
            	Integer currCnt = GenreCnt.get(genre) == null ? 0 : GenreCnt.get(genre);
            	GenreCnt.put(genre, currCnt + 1);
            	
            	Double genreAvgRating = GenreMap.get(genre) / GenreCnt.get(genre);
            	GenreAvgRating.put(genre, Double.parseDouble(format.format(genreAvgRating)));
            }
            
        }
        
        SortedGenreAvgRating = sortMap(GenreAvgRating);
        GenreRatingsTable.put(key.toString(), GenreMap);
		//context.write(key, new Text(GenreAvgRating.toString()));
		//context.write(key, new Text(SortedGenreAvgRating.toString()));
*/        
		//context.write(new Text(AGOC + "\n" + "--------------------" + "\n"), new Text(SortedGenreAvgRating.toString() + "\n"));
	        
	}
	
/*	public LinkedHashMap<String, Double> sortMap(TreeMap<String, Double> GenreRatingMap)
	{
		LinkedHashMap<String, Double> SortedMap = new LinkedHashMap<String, Double>();
		List<Map.Entry<String, Double>> list = new LinkedList<Map.Entry<String, Double>>(GenreRatingMap.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Double>>()
				{
					public int compare(Map.Entry<String, Double> r1, Map.Entry<String, Double> r2)
					{
						return r2.getValue().compareTo(r1.getValue());
					}
				});
		
		for(Map.Entry<String, Double> entry : list)
		{
			SortedMap.put(entry.getKey(), entry.getValue());
		}
		return SortedMap;
	}
*/	
}
