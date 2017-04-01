package edu.itu.csc.constants;

import java.math.BigInteger;

import edu.itu.csc.driver.SmartSortDriver;
import edu.itu.csc.generate.SmartSortGenerateData;
import edu.itu.csc.validators.SmartSortPostValidator;
import edu.itu.csc.validators.SmartSortPreValidator;

/**
 * Constants used in SmartSort.
 * 
 * @author "Jigar Gosalia"
 *
 */
public class Constants {

	// Hadoop Job Names
	public static final String SMART_SORT_PRE_VALIDATOR_JOB = "SmartSortPreValidator";

	public static final String SMART_SORT_DRIVER_JOB = "SmartSortDriverJob";

	public static final String SMART_SORT_POST_VALIDATOR_JOB = "SmartSortPostValidator";

	// Arguments Name
	public static final String REDUCERS = "reducers";

	public static final String INPUT = "input";

	public static final String OUTPUT = "output";

	public static final String PRIMARY = "primary";

	public static final String SECONDARY = "secondary";

	public static final String PRIMARY_ORDER = "porder";

	public static final String SECONDARY_ORDER = "sorder";

	public static final String TOTAL = "total";

	public static final String DELIMITER = "delimiter";

	public static final String SPLITS = "splits";

	// Arguments Options
	public static final String REDUCERS_OPTION = "-reducers";

	public static final String INPUT_OPTION = "-input";

	public static final String OUTPUT_OPTION = "-output";

	public static final String PRIMARY_OPTION = "-primary";

	public static final String SECONDARY_OPTION = "-secondary";

	public static final String PRIMARY_ORDER_OPTION = "-porder";

	public static final String SECONDARY_ORDER_OPTION = "-sorder";

	public static final String TOTAL_OPTION = "-total";

	public static final String DELIMITER_OPTION = "-delimiter";

	public static final String SPLITS_OPTION = "-splits";

	// Hadoop Command Usage
	public static final String SMART_SORT_GENERATE_DATA_USAGE = "\n\nUSAGE: "
			+ "hadoop jar <path to jar on local disk> "
			+ SmartSortGenerateData.class.getCanonicalName() + "\n"
			+ Constants.INPUT_OPTION + " <comma separated list of absolute input file paths on local> \n\t"
			+ Constants.OUTPUT_OPTION + " <path to output directory on hdfs> \n\t"
			+ Constants.DELIMITER_OPTION + " <delimiter> \n\t"
			+ Constants.SPLITS_OPTION + " <splits>";

	public static final String SMART_SORT_PRE_VALIDATOR_USAGE = "\n\nUSAGE: "
			+ "hadoop jar <path to jar on local disk> "
			+ SmartSortPreValidator.class.getCanonicalName() + "\n"
			+ Constants.INPUT_OPTION + " <path to input file on hdfs> \n\t"
			+ Constants.OUTPUT_OPTION + " <path to output directory on hdfs> \n\t"
			+ Constants.TOTAL_OPTION + " <total no. of keys> \n\t"
			+ Constants.DELIMITER_OPTION + " <delimiter>";

	public static final String SMART_SORT_USAGE = "\n\nUSAGE: "
			+ "hadoop jar <path to jar on local disk> "
			+ SmartSortDriver.class.getCanonicalName() + "\n\t"
			+ " [" + Constants.REDUCERS_OPTION + " <no. of reducers>] \n\t"
			+ Constants.INPUT_OPTION + " <path to input file on hdfs> \n\t"
			+ Constants.OUTPUT_OPTION + " <path to output directory on hdfs> \n\t"
			+ Constants.PRIMARY_OPTION + " <primary-key> \n\t"
			+ Constants.SECONDARY_OPTION + " <secondary-key> \n\t"
			+ " [" + Constants.PRIMARY_ORDER_OPTION + " <primary-key order asc/desc>] \n\t"
			+ " [" + Constants.SECONDARY_ORDER_OPTION + " <secondary-key order asc/desc>] \n\t"
			+ Constants.TOTAL_OPTION + " <total no. of keys> \n\t"
			+ Constants.DELIMITER_OPTION + " <delimiter>\n\n";

	public static final String SMART_SORT_POST_VALIDATOR_USAGE = "\n\nUSAGE: "
			+ "hadoop jar <path to jar on local disk> "
			+ SmartSortPostValidator.class.getCanonicalName() + "\n\t"
			+ Constants.INPUT_OPTION + " <path to input file on hdfs> \n\t"
			+ Constants.OUTPUT_OPTION + " <path to output directory on hdfs> \n\t"
			+ Constants.TOTAL_OPTION + " <total no. of keys> \n\t"
			+ " [" + Constants.PRIMARY_ORDER_OPTION + " <primary-key order asc/desc>] \n\t"
			+ " [" + Constants.SECONDARY_ORDER_OPTION + " <secondary-key order asc/desc>] \n\t"
			+ Constants.DELIMITER_OPTION + " <delimiter>\n\n";

	// Errors, Exception and Success Messages
	public static final String MANDATORY_PARAMETERS_ERROR = "\n\nMISSING FIELD OR ERROR FIELD: <%s> \n\n";

	public static final String UNKNOWN_PARAMETER_EXCEPTION = "\n\nERROR: Unknown Parameter : <%s>!!\n\n";

	public static final String INTEGER_EXPECTED_EXCEPTION = "\n\nERROR: Integer expected instead of <%s>!!\n\n";

	public static final String REQUIRED_PARAMETER_EXCEPTION = "\n\nERROR: Required parameter missing from <%s>!!\n\n";

	public static final String GENERAL_EXCEPTION = "\n\nERROR: <%s>!!\n\n";

	public static final String COMPARISON_FAILED_ERROR = "\n\nERROR: Comparison failed : \n\tFILENAME = %s \n\tPORDER   = %s \n\tSORDER   = %s \n\tPREVIOUS = %s \n\tCURRENT  = %s \n\n";

	public static final String DATA_INCONSISTENT_ERROR = "\n\nERROR: Count of data fields do not match total fields : \n\tFILENAME  = %s \n\tDELIMITER = %s \n\tVALUE     = %s \n\n";

	public static final String SEED_DATA_INCONSISTENT_ERROR = "\n\nERROR: Seed Data Input File contains DELIMITER : \n\tFILENAME  = %s \n\tDELIMITER = %s \n\tVALUE     = %s \n\n";

	public static final String INVALID_DATA_ERROR = "\n\nERROR: Either no data or data doesn't have delimiter : \n\tFILENAME  = %s \n\tDELIMITER = %s \n\tVALUE     = %s \n\n";

	public static final String PRE_VALIDATION_SUCCESSFUL = "\t!!Smart Sort Pre Validation Successful!!\t";

	public static final String PRE_VALIDATION_FAILED = "\t!!Smart Sort Pre Validation Failed!!\t";

	public static final String POST_VALIDATION_SUCCESSFUL = "\t!!Smart Sort Post Validation Successful!!\t";

	public static final String POST_VALIDATION_FAILED = "\t!!Smart Sort Post Validation Failed!!\t";

	public static final String INCONSISTENT_VALUES_ERROR = "incosistent values of -primary, -secondary or -total";

	public static final String INCONSISTENT_TOTAL_ERROR = "check value of -total";

	// Symbols
	public static final String COMMA = ",";

	public static final String SPACE = " ";

	public static final String EMPTY = "";

	public static final String NEW_LINE = "\n";

	public static final String COLON = ";";

	public static final String SEMI_COLON = ";";

	public static final String ZERO = "0";

	public static final String ONE = "1";
	
	// Display Strings
	public static final String LINE = "\n=======================================================\n";

	public static final String DISPLAY_INPUT = "========= Displaying Input Arguments ==================";

	public static final String DISPLAY_STATISTICS = "========= Displaying Data Statistics ==================";
	
	public static final String DISPLAY_ARGUMENTS = "\t %-20s : %s \t";

	public static final String INPUT_DETAILS = "input details";

	public static final String CALCULATED_RECORDS = "calculated records";

	public static final String FIELDS_PER_RECORD = "fields / record";

	public static final String TOTAL_FILES = "total files";

	public static final String TOTAL_RECORDS = "total records";

	public static final String TOTAL_SPACE = "total space";

	public static final String DATA_USAGE = "%s MB (%s GB)";

	// Default Values
	public static final String ASC = "asc";

	public static final String DESC = "desc";

	public static final String DEFAULT_REDUCERS = "128";

	public static final String DEFAULT_PRIMARY_KEY_ORDER = ASC;

	public static final String DEFAULT_SECONDARY_KEY_ORDER = DESC;

	// HDFS File Related Attributes
	public static final BigInteger BYTE_UNIT = new BigInteger(String.valueOf(1024));

	public static final BigInteger ONE_MB = BYTE_UNIT.multiply(BYTE_UNIT);

	public static final BigInteger HUNDRED_MB = ONE_MB.multiply(BigInteger.TEN).multiply(BigInteger.TEN);

	public static final BigInteger GIGABIT = ONE_MB.multiply(BYTE_UNIT);

	public static final BigInteger DEFAULT_MAX_FILE_SIZE_MB = HUNDRED_MB;

	public static final int DEFAULT_SPLITS = 103;

	public static final String FILE_SEPARATOR = "file.separator";

	public static final String OUTPUT_FILE_PATTERN = "part-i-%05d.txt";
}
