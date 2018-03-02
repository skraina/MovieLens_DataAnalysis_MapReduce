package com.github.skraina.movielens.toprated;

//import java.io.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;


public class SortFloatWritableComparator extends WritableComparator 
{
	protected SortFloatWritableComparator()
	{
		super(FloatWritable.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		FloatWritable k1 = (FloatWritable) w1;
		FloatWritable k2 = (FloatWritable) w2;
		
		return -1 * k1.compareTo(k2);
	}
}
