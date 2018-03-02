package com.github.skraina.movielens.rankgenre;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortingComparatorUID extends WritableComparator 
{
	protected SortingComparatorUID()
	{
		super(CompositeKeyWritableUID.class, true);
	}
	
	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable wc1, @SuppressWarnings("rawtypes") WritableComparable wc2)
	{
		CompositeKeyWritableUID CKey1 = (CompositeKeyWritableUID) wc1;
		CompositeKeyWritableUID CKey2 = (CompositeKeyWritableUID) wc2;
		
		//int result = CKey1.getjoinKey().compareTo(CKey2.getjoinKey());
		
		// Splitting at "-" in CKey.getJoinKey only for Reducer in MLRGByAgeOccpReducer
		// so that [movieID GenreRank], [movieID-AG RatingCount], [movieID-OC RatingCount]
		// can be joined. This general splitting will not effect CKeys in other datasets
		// such as ratings.dat and users.dat as there is no "-" in userID.
		int result = CKey1.getjoinKey().split("-")[0].compareTo(CKey2.getjoinKey().split("-")[0]);
		if(result == 0) // same userID or movieID in datasets ratings, users, and movies
		{
			return Double.compare(CKey1.getFileIndex(), CKey2.getFileIndex());
		}
		return result;
	}

}
