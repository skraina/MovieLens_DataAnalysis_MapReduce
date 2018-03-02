package com.github.skraina.movielens.toprated;

//Custom writable which creates composite key with movieID and FileIndex

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritableMID implements WritableComparable<CompositeKeyWritableMID> 
{
	private String joinKey; // movieID
	private int FileIndex; // 1:movies.dat, 2:ratings.dat
	
	public CompositeKeyWritableMID()
	{
		
	}
	
	public CompositeKeyWritableMID(String joinKey, int FileIndex)
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
	
	public int compareTo(CompositeKeyWritableMID KeyPair)
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
