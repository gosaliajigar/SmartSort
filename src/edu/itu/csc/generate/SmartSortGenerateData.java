package edu.itu.csc.generate;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import edu.itu.csc.common.SmartSortUtilities;
import edu.itu.csc.constants.Constants;
import edu.itu.csc.pojos.InputDataFile;

/**
 * 
 * Random Data Generator for SmartSort.<br>
 * <br>
 * 
 * Given input seed files with individual field, this program generates comma
 * separated files which can be used for sorting.<br>
 * <br>
 * 
 * Each file will be max Constants.DEFAULT_MAX_FILE_SIZE_MB in size and total
 * no. of records in all the files will be Cartesian Product of records in the
 * individual field input files.<br>
 * <br>
 * 
 * Program will successfully terminate when ... <br><br>
 * &emsp;&emsp;1. Total Cartesian Product records exhaust before no. of splits
 * are reached (at most one file could be less than
 * Constants.DEFAULT_MAX_FILE_SIZE_MB)<br><br>
 * &emsp;&emsp;2. Total no. of splits are reached (Default Splits =
 * Constants.DEFAULT_SPLITS)<br><br>
 * <br>
 * 
 * @author "Jigar Gosalia"
 * 
 */
public class SmartSortGenerateData {

	public static int splitCount = 0;

	public static BigInteger totalSpace = new BigInteger(String.valueOf(Constants.ZERO));

	public static BigInteger totalRecords = new BigInteger(String.valueOf(Constants.ZERO));

	public static BigInteger cartesianProduct = new BigInteger(String.valueOf(Constants.ONE));

	public static String delimiter = Constants.EMPTY;

	public static Map<String, String> arguments = new HashMap<String, String>();

	public static List<InputDataFile> inputFileNameDataList = new LinkedList<InputDataFile>();

	public static final List<String> mandatoryFields = new LinkedList<String>();

	static {
		mandatoryFields.add(Constants.INPUT);
		mandatoryFields.add(Constants.OUTPUT);
		mandatoryFields.add(Constants.DELIMITER);
		mandatoryFields.add(Constants.SPLITS);
	}

	/**
	 * Initialize Parameters.
	 * 
	 * @param args
	 */
	private static void init(String[] args) {
		arguments.put(Constants.SPLITS, String.valueOf(Constants.DEFAULT_SPLITS));
		arguments.putAll(SmartSortUtilities.parseArguments(args));
		delimiter = arguments.get(Constants.DELIMITER);
	}

	/**
	 * Read comma separated files with seed data.
	 * 
	 * @throws Exception
	 */
	private static void readFiles() throws Exception {
		String[] fileNames = arguments.get(Constants.INPUT).split(Constants.COMMA);
		for (String path : fileNames) {
			String directoryName = path.substring(0, path.lastIndexOf(System.getProperty(Constants.FILE_SEPARATOR)) + 1);
			String fileName = path.substring(path.lastIndexOf(System.getProperty(Constants.FILE_SEPARATOR)) + 1);
			if (directoryName != null && directoryName.length() > 0
					&& fileName != null && fileName.length() > 0) {
				List<String> data = readFile(directoryName, fileName);
				if (data != null && data.size() > 0) {
					inputFileNameDataList.add(new InputDataFile(path, new Random(), data));
				}
			}
		}
	}

	/**
	 * Read files line by line and skip if any line in a file contains DELIMITER.
	 * 
	 * @param directoryName
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	private static List<String> readFile(String directoryName, String fileName) throws Exception {
		List<String> fields = new LinkedList<String>();
		File directory = new File(directoryName);
		File file = new File(directory, fileName);
		FileReader reader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(reader);
		String field = null;
		while ((field = bufferedReader.readLine()) != null) {
			if (field.contains(arguments.get(Constants.DELIMITER))) {
				StringBuilder data = new StringBuilder();
				data.append(String.format(Constants.SEED_DATA_INCONSISTENT_ERROR, (directoryName + fileName), arguments.get(Constants.DELIMITER), field));
				throw new InterruptedIOException(data.toString());
			}
			if (field.replace(Constants.SPACE, Constants.EMPTY).length() > 0) {
				fields.add(field);
			}
		}
		cartesianProduct = cartesianProduct.multiply(new BigInteger(String.valueOf(fields.size())));
		return fields;
	}

	/**
	 * Generate data by Cartesian Product of seed data.
	 */
	private static void generateData() throws Exception {
		int fileSize = 0;
		List<String> records = new ArrayList<String>();
		for (BigInteger count = BigInteger.ZERO; count.compareTo(cartesianProduct) == -1; count = count.add(BigInteger.ONE)) {
			StringBuilder record = new StringBuilder();
			for (int index = 0; index < inputFileNameDataList.size(); index ++) {
				int maxRandomNumber = inputFileNameDataList.get(index).getCount();
				int randomLocation = inputFileNameDataList.get(index).getRandom().nextInt(maxRandomNumber);
				String randomField = inputFileNameDataList.get(index).getFields().get(randomLocation);
				record.append(randomField).append(delimiter);
			}
			if (record.charAt(record.length() - 1) == delimiter.charAt(0)) {
				record.deleteCharAt(record.length() - 1);
			}
			records.add(record.toString() + Constants.NEW_LINE);
			fileSize += record.toString().getBytes().length;
			// If exceeds DEFAULT_MAX_FILE_SIZE_MB then write records to file
			if (((new BigInteger(String.valueOf(fileSize))).compareTo(Constants.DEFAULT_MAX_FILE_SIZE_MB)) > 0) {
				splitCount++;
				writeBuffered(arguments.get(Constants.OUTPUT), String.format(Constants.OUTPUT_FILE_PATTERN, splitCount), records);
				totalRecords = totalRecords.add(new BigInteger(String.valueOf(records.size())));
				totalSpace = totalSpace.add(new BigInteger(String.valueOf(fileSize)));;
				// clear records for next write
				records.clear();
				fileSize = 0;
				// stop on expected no. of splits
				if (splitCount >= Integer.parseInt(arguments.get(Constants.SPLITS))) {
					break;
				}
			}
		}
		// What if fileSize is less than DEFAULT_MAX_FILE_SIZE_MB but greater than 0MB
		// Here split count will be checked to ensure we do not generate (splits + 1) files
		if (fileSize > 0) {
			splitCount++;
			if (splitCount <= Integer.parseInt(arguments.get(Constants.SPLITS))) {
				writeBuffered(arguments.get(Constants.OUTPUT), String.format(Constants.OUTPUT_FILE_PATTERN, splitCount), records);
				totalRecords = totalRecords.add(new BigInteger(String.valueOf(records.size())));
				totalSpace = totalSpace.add(new BigInteger(String.valueOf(fileSize)));;
			}
		}
	}

	/**
	 * Write data (equivalent to default max data) to file.
	 * 
	 * @param directoryName
	 * @param fileName
	 * @param records
	 * @param bufferSize
	 * @throws IOException
	 */
	private static void writeBuffered(String directoryName, String fileName, List<String> records) throws IOException {
		File directory = new File(directoryName);
		File file = new File(directory, fileName);
		directory.mkdirs();
		file.createNewFile();
		FileWriter writer = new FileWriter(file);
		BufferedWriter bufferedWriter = new BufferedWriter(writer);
		for (String record : records) {
			bufferedWriter.write(record);
		}
		bufferedWriter.flush();
		bufferedWriter.close();
	}

	/**
	 * Display Statistics for the data generation.
	 * 
	 */
	private static void displayStatistics() {
		System.out.println(Constants.LINE);
		System.out.println(Constants.DISPLAY_STATISTICS);
		System.out.println(Constants.LINE);
		StringBuilder inputDetails = new StringBuilder();
		for (InputDataFile inputFile : inputFileNameDataList) {
			String fileName = inputFile.getFileName().substring(inputFile.getFileName().lastIndexOf(System.getProperty(Constants.FILE_SEPARATOR)) + 1);
			inputDetails.append(fileName).append(Constants.COLON).append(inputFile.getCount()).append(Constants.SEMI_COLON);
		}
		System.out.println(String.format(Constants.DISPLAY_ARGUMENTS, Constants.INPUT_DETAILS, inputDetails.toString()));
		System.out.println(String.format(Constants.DISPLAY_ARGUMENTS, Constants.CALCULATED_RECORDS, cartesianProduct));
		System.out.println(String.format(Constants.DISPLAY_ARGUMENTS, Constants.FIELDS_PER_RECORD, inputFileNameDataList.size()));
		System.out.println(String.format(Constants.DISPLAY_ARGUMENTS, Constants.TOTAL_FILES, splitCount));
		System.out.println(String.format(Constants.DISPLAY_ARGUMENTS, Constants.TOTAL_RECORDS, totalRecords));
		System.out.println(String.format(Constants.DISPLAY_ARGUMENTS, Constants.TOTAL_SPACE, String.format(Constants.DATA_USAGE, new BigDecimal(totalSpace).divide(new BigDecimal(Constants.ONE_MB)).setScale(2, RoundingMode.HALF_UP).toString(), new BigDecimal(totalSpace).divide(new BigDecimal(Constants.GIGABIT)).setScale(2, RoundingMode.HALF_UP).toString())));
		System.out.println(Constants.LINE);
	}

	/**
	 * Main Method
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			init(args);

			if (SmartSortUtilities.hasValidArgumentsGenerate(arguments, mandatoryFields)) {
				SmartSortUtilities.printArguments(arguments);

				readFiles();

				generateData();

				displayStatistics();
			} else {
				SmartSortUtilities.printUsage(Constants.SMART_SORT_GENERATE_DATA_USAGE);
			}
		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}
}
