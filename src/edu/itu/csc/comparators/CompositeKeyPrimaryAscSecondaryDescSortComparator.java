package edu.itu.csc.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.itu.csc.key.CompositeKeyWritable;

/**
 * CompositeKey Sort Comparator to help sort data by first primary key (natural
 * key) and then secondary key (natural value).
 * 
 * @author "Jigar Gosalia"
 * 
 */
public class CompositeKeyPrimaryAscSecondaryDescSortComparator extends WritableComparator {

	/* Constructor */
	protected CompositeKeyPrimaryAscSecondaryDescSortComparator() {
		super(CompositeKeyWritable.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;

		// compare primary key
		int result = key1.getPrimaryKey().compareTo(key2.getPrimaryKey());
		// if primary key matches, compare secondary key
		if (result == 0) {
			// values will be in descending order
			return (-1) * key1.getSecondaryKey().compareTo(key2.getSecondaryKey());
		}
		return result;
	}
}