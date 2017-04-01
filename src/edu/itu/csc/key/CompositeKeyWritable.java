package edu.itu.csc.key;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Composite Key for SmartSort.
 * 
 * @author "Jigar Gosalia"
 * 
 */
public class CompositeKeyWritable implements Writable,
		WritableComparable<CompositeKeyWritable> {

	/* natural key */
	private String primaryKey;

	/* natural value */
	private String secondaryKey;

	/* delimiter */
	private String delimiter;

	/* Constructor */
	public CompositeKeyWritable() {}

	/* Constructor */
	public CompositeKeyWritable(String primaryKey, String secondaryKey) {
		this.primaryKey = primaryKey;
		this.secondaryKey = secondaryKey;
	}

	/* Constructor */
	public CompositeKeyWritable(String primaryKey, String secondaryKey, String delimiter) {
		this.primaryKey = primaryKey;
		this.secondaryKey = secondaryKey;
		this.delimiter = delimiter;
	}

	public String getPrimaryKey() {
		return this.primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getSecondaryKey() {
		return this.secondaryKey;
	}

	public void setSecondaryKey(String secondaryKey) {
		this.secondaryKey = secondaryKey;
	}

	public String getDelimiter() {
		return this.delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	@Override
	public String toString() {
		return (this.primaryKey + this.delimiter + this.secondaryKey);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.primaryKey = WritableUtils.readString(dataInput);
		this.delimiter = WritableUtils.readString(dataInput);
		this.secondaryKey = WritableUtils.readString(dataInput);
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		WritableUtils.writeString(dataOutput, this.primaryKey);
		WritableUtils.writeString(dataOutput, this.delimiter);
		WritableUtils.writeString(dataOutput, this.secondaryKey);
	}

	@Override
	public int compareTo(CompositeKeyWritable compositeKey) {
		// compare primary key
		int result = this.primaryKey.compareTo(compositeKey.getPrimaryKey());
		// if primary matches then compare secondary key
		if (result == 0) {
			result = this.secondaryKey.compareTo(compositeKey.getSecondaryKey());
		}
		return result;
	}
}