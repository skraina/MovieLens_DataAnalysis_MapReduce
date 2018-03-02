package com.github.skraina.movielens.rankgenre;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class CombinerKeyGroupingComparatorUID extends WritableComparator
{
	protected CombinerKeyGroupingComparatorUID()
	{
		super(CompositeKeyWritableUID.class, true);
	}
	
	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable w1, @SuppressWarnings("rawtypes") WritableComparable w2)
	{
		CompositeKeyWritableUID CKey1 = (CompositeKeyWritableUID) w1;
		CompositeKeyWritableUID CKey2 = (CompositeKeyWritableUID) w2;

		return CKey1.getjoinKey().compareTo(CKey2.getjoinKey());
	}

}
