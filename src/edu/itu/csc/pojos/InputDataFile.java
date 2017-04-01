package edu.itu.csc.pojos;

import java.util.List;
import java.util.Random;

/**
 * POJO for holding input file name, random number generator and data from file.
 * 
 * @author "Jigar Gosalia"
 *
 */
public class InputDataFile {

	/* holds fileName */
	private String fileName;

	/* holds random number generator for data generation */
	private Random random;

	/* holds file data */
	private List<String> fields;

	/* holds line count of file data */
	private int count;

	public InputDataFile(String fileName, Random random, List<String> fields) {
		super();
		this.fileName = fileName;
		this.random = random;
		this.fields = fields;
		this.count = (this.fields != null && this.fields.size() > 0) ? fields.size() : 0;
	}

	public String getFileName() {
		return fileName;
	}

	public Random getRandom() {
		return random;
	}

	public List<String> getFields() {
		return fields;
	}

	public int getCount() {
		return count;
	}
}
