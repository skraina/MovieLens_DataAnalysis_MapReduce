package com.github.skraina.movielens.rankgenre;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class GroupingComparatorUID extends WritableComparator
{
	protected GroupingComparatorUID()
	{
		super(CompositeKeyWritableUID.class, true);
	}
	
	@Override
	public int compare(@SuppressWarnings("rawtypes") WritableComparable w1, @SuppressWarnings("rawtypes") WritableComparable w2)
	{
		CompositeKeyWritableUID CKey1 = (CompositeKeyWritableUID) w1;
		CompositeKeyWritableUID CKey2 = (CompositeKeyWritableUID) w2;
		
		// Splitting at "-" in CKey.getJoinKey only for Reducer in MLRGByAgeOccpReducer
		// so that [movieID GenreRank], [movieID-AG RatingCount], [movieID-OC RatingCount]
		// can be joined. This general splitting will not effect CKeys in other datasets
		// such as ratings.dat and users.dat as there is no "-" in userID.
		return CKey1.getjoinKey().split("-")[0].compareTo(CKey2.getjoinKey().split("-")[0]);
/*		int result =  CKey1.getjoinKey().split("-")[0].compareTo(CKey2.getjoinKey().split("-")[0]);
		if(result == 0)
		{
			return Double.compare(CKey1.getFileIndex(), CKey2.getFileIndex());
		}
		if(result == 0) // same userID or movieID in datasets ratings, users, and movies
		{
			result = Double.compare(CKey1.getFileIndex(), CKey2.getFileIndex());
			if(result == 0)
			{
				if(CKey1.getFileIndex() == 2)
				{
					return result;
				}
				else
				{
					CKey1.getjoinKey().split("-")[1].compareTo(CKey2.getjoinKey().split("-")[1]);
				}
				
			}
			return result;
		}
		return result;
*/
	}

}
