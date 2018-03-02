package com.github.skraina.movielens.topviewed;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerMID extends Partitioner<CompositeKeyWritableMID, Text> 
{
	@Override
	public int getPartition(CompositeKeyWritableMID CKey, Text value, int numReduceTasks)
	{
		// Partitions all keys based on the natural key which in this case is MovieID.
		return (CKey.getjoinKey().hashCode() % numReduceTasks);
	}

}
