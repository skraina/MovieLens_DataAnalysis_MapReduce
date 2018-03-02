package com.github.skraina.movielens.year;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieLensYearMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		// Map task reads each record in below format and sets Year as the key and movie name as
		// the value. After shuffling and sorting Reducer task receives all movie names released
		// in a particular year as the value for that year.
		String line = value.toString();
//		1::Toy Story (1995)::Animation|Children's|Comedy
		
		final String[] data = line.trim().split("::");
		// data[0] = 1
		// data[1] = Toy Story (1995)
		// data[2] = Animation|Children's|Comedy
	
		if(data.length != 3 || (data[0].isEmpty() || data[1].isEmpty() || data[2].isEmpty()))
		{
			// If movie records does not contain all the three fields then records directly go
			// to Bad Records folder inside output directory in HDFS
			context.write(new Text("Duplicate Movie ID"), value);
			
		}
		else
		{
			// Since movie name and year are together, a regex is used to extract year.
			// For an example string Toy Story (1995) regex is composed of lookbehind 
			// assertion ?<=\\( followed by lookahead assertion ?=\\d{4} for four digits
			// of year OR ?=\\d{4} followed by ?=\\) as ending parenthesis.
			String movie[] = data[1].split("(?<=\\()(?=\\d{4})|(?<=\\d{4})(?=\\))");
			String year = movie[1];
			String name = data[1].replace("("+year+")", "");
			
			context.write(new Text(year), new Text(name));
		}
	}

}
