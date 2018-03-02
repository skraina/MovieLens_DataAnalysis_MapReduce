package com.github.skraina.movielens.rankgenre;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class SortingComparatorAGOC extends WritableComparator 
{
	protected SortingComparatorAGOC()
	{
		super(CompositeKeyWritableAGOC.class, true);
	}
	
	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable wc1, @SuppressWarnings("rawtypes") WritableComparable wc2)
	{
		CompositeKeyWritableAGOC key1 = (CompositeKeyWritableAGOC) wc1;
		CompositeKeyWritableAGOC key2 = (CompositeKeyWritableAGOC) wc2;
		
		int result = key1.getjoinKey().compareTo(key2.getjoinKey());
		if(result == 0) // same userID in two datasets ratings and users
		{
			return -1 * Double.compare(key1.getRatingAvg(), key2.getRatingAvg());
		}
		return result;
	}


}
