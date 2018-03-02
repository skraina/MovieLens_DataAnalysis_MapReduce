package com.github.skraina.movielens.toprated;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class GroupingComparatorMID extends WritableComparator
{
	protected GroupingComparatorMID()
	{
		super(CompositeKeyWritableMID.class, true);
	}
	
	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable w1, @SuppressWarnings("rawtypes") WritableComparable w2)
	{
		CompositeKeyWritableMID CKey1 = (CompositeKeyWritableMID) w1;
		CompositeKeyWritableMID CKey2 = (CompositeKeyWritableMID) w2;
		return CKey1.getjoinKey().compareTo(CKey2.getjoinKey());
	}

}