package com.github.skraina.movielens.rankgenre;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerUID extends Partitioner<CompositeKeyWritableUID, Text> 
{
	@Override
	public int getPartition(CompositeKeyWritableUID CKey, Text value, int numReduceTasks)
	{
		return (Math.abs(CKey.getjoinKey().split("-")[0].hashCode()) % numReduceTasks);
		//return (Math.abs(CKey.getjoinKey().split("[A-Z]")[0].hashCode()) % numReduceTasks);

	}

}
