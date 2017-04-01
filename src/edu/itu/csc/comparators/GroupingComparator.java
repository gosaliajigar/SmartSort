package edu.itu.csc.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.itu.csc.key.CompositeKeyWritable;

/**
 * Grouping Comparator for group by primary key (natural key).
 * 
 * @author "Jigar Gosalia"
 * 
 */
public class GroupingComparator extends WritableComparator {

	/* Constructor */
	protected GroupingComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
		return key1.getPrimaryKey().compareTo(key2.getPrimaryKey());
	}
}