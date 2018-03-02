package com.github.skraina.movielens.toprated;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortingComparatorMID extends WritableComparator 
{
	protected SortingComparatorMID()
	{
		super(CompositeKeyWritableMID.class, true);
	}
	
	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable wc1, @SuppressWarnings("rawtypes") WritableComparable wc2)
	{
		CompositeKeyWritableMID key1 = (CompositeKeyWritableMID) wc1;
		CompositeKeyWritableMID key2 = (CompositeKeyWritableMID) wc2;
		
		int result = key1.getjoinKey().compareTo(key2.getjoinKey());
		if(result == 0) // same userID in two datasets ratings and users
		{
			return Double.compare(key1.getFileIndex(), key2.getFileIndex());
		}
		return result;
	}

}
