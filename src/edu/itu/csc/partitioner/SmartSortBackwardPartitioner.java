package edu.itu.csc.partitioner;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.itu.csc.key.CompositeKeyWritable;

/**
 * Partitioner to identify which partition data should go.
 * 
 * @author "Jigar Gosalia"
 * 
 */
public class SmartSortBackwardPartitioner extends
		Partitioner<CompositeKeyWritable, NullWritable> {

	@Override
	public int getPartition(CompositeKeyWritable key, NullWritable value,
			int numReduceTasks) {
		// Use the first character's ASCII value to identify the partition so all 
		// lines with same starting character will be placed in same file.
		// Subtract them so that it can be sorted in right order.
		return (numReduceTasks - (Integer.valueOf(key.getPrimaryKey().charAt(0)) % numReduceTasks));
	}
}