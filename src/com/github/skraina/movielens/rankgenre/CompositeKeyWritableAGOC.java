package com.github.skraina.movielens.rankgenre;

//Custom writable which creates composite key with AG or OC and Average of rating.

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritableAGOC implements WritableComparable<CompositeKeyWritableAGOC> 
{
	private String joinKey; // Either Age Group AG or Occupation Code OC
	private float RatingAvg; // Average of rating for each AG and OC
	
	public CompositeKeyWritableAGOC()
	{
	}
	
	public CompositeKeyWritableAGOC(String joinKey, int RatingAvg)
	{
		this.joinKey = joinKey;
		this.RatingAvg = RatingAvg;
	}
	
	public void readFields(DataInput in) throws IOException
	{
		joinKey = WritableUtils.readString(in);
		//RatingAvg = WritableUtils.readVInt(in);
		RatingAvg = in.readFloat();
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		WritableUtils.writeString(out, joinKey);
		out.writeFloat(RatingAvg);
		//WritableUtils.writeString(out, Float.toString(RatingAvg));
	}
	
	public int compareTo(CompositeKeyWritableAGOC KeyPair)
	{
		int result = joinKey.compareTo(KeyPair.joinKey);
		if(result == 0)
			result = Double.compare(RatingAvg, KeyPair.RatingAvg);
		return result;
	}
	
	public String getjoinKey()
	{
		return joinKey;
	}
	
	public void setjoinKey(String joinKey)
	{
		this.joinKey = joinKey;
	}
	
	public float getRatingAvg()
	{
		return RatingAvg;
	}
	
	public void setRatingAvg(float RatingAvg)
	{
		this.RatingAvg = RatingAvg;
	}
	
	@Override
	public int hashCode()
	{
		return joinKey.hashCode() * 163 + Float.toString(RatingAvg).hashCode();
	}

	@Override
	public String toString()
	{
		return joinKey + "\t" + Float.toString(RatingAvg);
	}


}
