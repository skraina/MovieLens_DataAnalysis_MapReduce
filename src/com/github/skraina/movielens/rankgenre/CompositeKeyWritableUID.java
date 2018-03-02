package com.github.skraina.movielens.rankgenre;

// Custom writable which creates composite key with userID and FileIndex

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritableUID implements WritableComparable<CompositeKeyWritableUID> 
{
/*	private String joinKey; // userID
	private int FileIndex; // 1:ratings.dat, 2:users.dat
	private String AGOC;  // AGOC represents either Age group or the Occupation code. Ratings for 
	// each movie needs to be partitioned among AG and OC.
	
	public CompositeKeyWritableUID()
	{
		
	}
	
	public CompositeKeyWritableUID(String joinKey, int FileIndex, String AGOC)
	{
		this.joinKey = joinKey;
		this.FileIndex = FileIndex;
		this.AGOC = AGOC;
	}
	
	public void readFields(DataInput in) throws IOException
	{
		joinKey = WritableUtils.readString(in);
		FileIndex = WritableUtils.readVInt(in);
		AGOC = WritableUtils.readString(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		WritableUtils.writeString(out, joinKey);
		WritableUtils.writeVInt(out, FileIndex);
		WritableUtils.writeString(out, AGOC);
	}
	
	public int compareTo(CompositeKeyWritableUID KeyPair)
	{
		int result = joinKey.compareTo(KeyPair.joinKey);
		if(result == 0)
			result = Double.compare(FileIndex, KeyPair.FileIndex);
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
	
	public int getFileIndex()
	{
		return FileIndex;
	}
	
	public void setFileIndex(int FileIndex)
	{
		this.FileIndex = FileIndex;
	}
	
	public String getAGOC()
	{
		return AGOC;
	}
	public void setAGOC(String AGOC)
	{
		this.AGOC = AGOC;
	}
	
	@Override
	public int hashCode()
	{
		return joinKey.hashCode() * 31 + Integer.toString(FileIndex).hashCode() + AGOC.hashCode();
	}

	@Override
	public String toString()
	{
		return joinKey + "\t" + Integer.toString(FileIndex) + "\t" + AGOC;
	}
*/
	private String joinKey; // userID OR movieID
	private int FileIndex; // 1:ratings.dat, 2:users.dat 3:movies.dat
	
	public CompositeKeyWritableUID()
	{
		
	}
	
	public CompositeKeyWritableUID(String joinKey, int FileIndex)
	{
		this.joinKey = joinKey;
		this.FileIndex = FileIndex;
	}
	
	public void readFields(DataInput in) throws IOException
	{
		joinKey = WritableUtils.readString(in);
		FileIndex = WritableUtils.readVInt(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		WritableUtils.writeString(out, joinKey);
		WritableUtils.writeVInt(out, FileIndex);
	}
	
	public int compareTo(CompositeKeyWritableUID KeyPair)
	{
		int result = joinKey.compareTo(KeyPair.joinKey);
		if(result == 0)
			result = Double.compare(FileIndex, KeyPair.FileIndex);
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
	
	public int getFileIndex()
	{
		return FileIndex;
	}
	
	public void setFileIndex(int FileIndex)
	{
		this.FileIndex = FileIndex;
	}
	
	@Override
	public int hashCode()
	{
		return joinKey.hashCode() * 163 + Integer.toString(FileIndex).hashCode();
	}

	@Override
	public String toString()
	{
		return joinKey + "\t" + Integer.toString(FileIndex);
	}

}
