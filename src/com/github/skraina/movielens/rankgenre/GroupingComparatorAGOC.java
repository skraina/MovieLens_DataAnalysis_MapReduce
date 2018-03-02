package com.github.skraina.movielens.rankgenre;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;


public class GroupingComparatorAGOC extends WritableComparator 
{
	protected GroupingComparatorAGOC()
	{
		super(CompositeKeyWritableAGOC.class, true);
	}
	
	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable w1, @SuppressWarnings("rawtypes") WritableComparable w2)
	{
		CompositeKeyWritableAGOC CKey1 = (CompositeKeyWritableAGOC) w1;
		CompositeKeyWritableAGOC CKey2 = (CompositeKeyWritableAGOC) w2;
		return CKey1.getjoinKey().compareTo(CKey2.getjoinKey());
	}


}
