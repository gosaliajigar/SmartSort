package edu.itu.csc.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.itu.csc.comparators.CompositeKeyPrimaryAscSecondaryAscSortComparator;
import edu.itu.csc.comparators.CompositeKeyPrimaryAscSecondaryDescSortComparator;
import edu.itu.csc.comparators.CompositeKeyPrimaryDescSecondaryAscSortComparator;
import edu.itu.csc.comparators.CompositeKeyPrimaryDescSecondaryDescSortComparator;
import edu.itu.csc.constants.Constants;
import edu.itu.csc.key.CompositeKeyWritable;
import edu.itu.csc.partitioner.SmartSortBackwardPartitioner;
import edu.itu.csc.partitioner.SmartSortForwardPartitioner;

/**
 * Common Utilities Utility class for SmartSort.
 * 
 * @author "Jigar Gosalia"
 * 
 */
public class SmartSortUtilities {

	/**
	 * Print command-line and default values of arguments used for processing request.
	 * 
	 * @param arguments
	 */
	public static void printArguments(Map<String, String> arguments) {
		System.out.println(Constants.LINE);
		System.out.println(Constants.DISPLAY_INPUT);
		System.out.println(Constants.LINE);
		for (Map.Entry<String, String> entry : arguments.entrySet()) {
			System.out.println(String.format(Constants.DISPLAY_ARGUMENTS,
					entry.getKey(), entry.getValue()));
		}
		System.out.println(Constants.LINE);
	}

	/**
	 * Parse the command-line arguments.
	 * 
	 * @param arguments
	 * @return
	 */
	public static Map<String, String> parseArguments(String[] arguments) {
		Map<String, String> argumentsMap = new HashMap<String, String>();
		for (int i = 0; i < arguments.length; ++i) {
			try {
				if (Constants.REDUCERS_OPTION.equals(arguments[i])) {
					argumentsMap.put(Constants.REDUCERS, String.valueOf(Integer.parseInt(arguments[++i])));
				} else if (Constants.INPUT_OPTION.equals(arguments[i])) {
					argumentsMap.put(Constants.INPUT, arguments[++i]);
				} else if (Constants.OUTPUT_OPTION.equals(arguments[i])) {
					argumentsMap.put(Constants.OUTPUT, arguments[++i]);
				} else if (Constants.PRIMARY_OPTION.equals(arguments[i])) {
					argumentsMap.put(Constants.PRIMARY,	String.valueOf(Integer.parseInt(arguments[++i])));
				} else if (Constants.SECONDARY_OPTION.equals(arguments[i])) {
					argumentsMap.put(Constants.SECONDARY, String.valueOf(Integer.parseInt(arguments[++i])));
				} else if (Constants.PRIMARY_ORDER_OPTION.equals(arguments[i])) {
					argumentsMap.put(Constants.PRIMARY_ORDER,arguments[++i]);
				} else if (Constants.SECONDARY_ORDER_OPTION.equals(arguments[i])) {
					argumentsMap.put(Constants.SECONDARY_ORDER, arguments[++i]);
				} else if (Constants.TOTAL_OPTION.equals(arguments[i])) {
					argumentsMap.put(Constants.TOTAL, String.valueOf(Integer.parseInt(arguments[++i])));
				} else if (Constants.DELIMITER_OPTION.equals(arguments[i])) {
					argumentsMap.put(Constants.DELIMITER, arguments[++i]);
				} else if (Constants.SPLITS_OPTION.equals(arguments[i])) {
					argumentsMap.put(Constants.SPLITS, String.valueOf(Integer.parseInt(arguments[++i])));
				} else {
					throw new Exception(String.format(Constants.UNKNOWN_PARAMETER_EXCEPTION, arguments[i]));
				}
			} catch (ArrayIndexOutOfBoundsException exception) {
				System.out.println(String.format(Constants.REQUIRED_PARAMETER_EXCEPTION, arguments[i - 1]));
				exception.printStackTrace();
			} catch (Exception exception) {
				System.out.println(String.format(Constants.GENERAL_EXCEPTION, exception.toString()));
				exception.printStackTrace();
			}
		}
		return argumentsMap;
	}

	/**
	 * Print Hadoop Command Usage.
	 *
	 * @param usage
	 */
	public static void printUsage(String usage) {
		System.out.println(usage);
	}

	/**
	 * Get reducers from command-line arguments.
	 * 
	 * @param arguments
	 * @return
	 */
	public static int getReducers(Map<String, String> arguments) {
		return (Integer.parseInt(arguments.get(Constants.REDUCERS) != null 
				? arguments.get(Constants.REDUCERS)
				: Constants.DEFAULT_REDUCERS));
	}

	/**
	 * Get Partitioner as per ordering status of primary key.
	 * 
	 * @param arguments
	 * @return
	 */
	public static Class<? extends Partitioner<CompositeKeyWritable, NullWritable>> getPartitioner(Map<String, String> arguments) {
		if (arguments.get(Constants.PRIMARY_ORDER) != null
				&& Constants.DESC.equals(arguments.get(Constants.PRIMARY_ORDER))) {
			return SmartSortBackwardPartitioner.class;
		} else {
			return SmartSortForwardPartitioner.class;
		}
	}

	/**
	 * Get Sort Comparator as per ordering status of primary and secondary key.
	 * 
	 * @param arguments
	 * @return
	 */
	public static Class<? extends WritableComparator> getSortComparator(Map<String, String> arguments) {
		Class<? extends WritableComparator> clazz = CompositeKeyPrimaryAscSecondaryDescSortComparator.class;
		if (arguments.get(Constants.PRIMARY_ORDER) != null
				&& arguments.get(Constants.SECONDARY_ORDER) != null) {
			if (Constants.ASC.equals(arguments.get(Constants.PRIMARY_ORDER))
					&& Constants.ASC.equals(arguments.get(Constants.SECONDARY_ORDER))) {
				clazz = CompositeKeyPrimaryAscSecondaryAscSortComparator.class;
			} else if (Constants.ASC.equals(arguments.get(Constants.PRIMARY_ORDER))
					&& Constants.DESC.equals(arguments.get(Constants.SECONDARY_ORDER))) {
				clazz = CompositeKeyPrimaryAscSecondaryDescSortComparator.class;
			} else if (Constants.DESC.equals(arguments.get(Constants.PRIMARY_ORDER))
					&& Constants.ASC.equals(arguments.get(Constants.SECONDARY_ORDER))) {
				clazz = CompositeKeyPrimaryDescSecondaryAscSortComparator.class;
			} else if (Constants.DESC.equals(arguments.get(Constants.PRIMARY_ORDER))
					&& Constants.DESC.equals(arguments.get(Constants.SECONDARY_ORDER))) {
				clazz = CompositeKeyPrimaryDescSecondaryDescSortComparator.class;
			}
		}
		return clazz;
	}

	/**
	 * Check if the command-line arguments are valid for Generating Data.
	 * 
	 * @param arguments
	 * @return
	 */
	public static boolean hasValidArgumentsGenerate(Map<String, String> arguments, List<String> mandatoryAttributes) {
		String errorMessage = checkMandatoryAttributes(arguments, mandatoryAttributes);
		if (errorMessage.length() > 0) {
			System.out.println(String.format(Constants.MANDATORY_PARAMETERS_ERROR, errorMessage));
		}
		return (errorMessage.length() == 0);
	}

	/**
	 * Check if the command-line arguments are valid for PreSort Validation.
	 * 
	 * @param arguments
	 * @return
	 */
	public static boolean hasValidArgumentsPreSort(Map<String, String> arguments, List<String> mandatoryAttributes) {
		return (hasValidArgumentsValidators(arguments, mandatoryAttributes));
	}

	/**
	 * Check if the command-line arguments are valid for Sort.
	 * 
	 * @param arguments
	 * @return
	 */
	public static boolean hasValidArgumentsSort(Map<String, String> arguments, List<String> mandatoryAttributes) {
		String errorMessage = checkMandatoryAttributes(arguments, mandatoryAttributes);
		if (errorMessage.length() == 0) {
			if (Integer.parseInt(arguments.get(Constants.PRIMARY)) >= 0
					&& Integer.parseInt(arguments.get(Constants.SECONDARY)) >= 0
					&& !arguments.get(Constants.PRIMARY).equals(arguments.get(Constants.SECONDARY))
					&& Integer.parseInt(arguments.get(Constants.TOTAL)) > 0
					&& Integer.parseInt(arguments.get(Constants.TOTAL)) > Integer.parseInt(arguments.get(Constants.PRIMARY))
					&& Integer.parseInt(arguments.get(Constants.TOTAL)) > Integer.parseInt(arguments.get(Constants.SECONDARY))) {
				// do nothing
			} else {
				errorMessage = Constants.INCONSISTENT_VALUES_ERROR;
			}
		}
		if (errorMessage.length() > 0) {
			System.out.println(String.format(Constants.MANDATORY_PARAMETERS_ERROR, errorMessage));
		}
		return (errorMessage.length() == 0);
	}

	/**
	 * Check if the command-line arguments are valid for PostSort Validation.
	 * 
	 * @param arguments
	 * @return
	 */
	public static boolean hasValidArgumentsPostSort(Map<String, String> arguments, List<String> mandatoryAttributes) {
		return (hasValidArgumentsValidators(arguments, mandatoryAttributes));
	}

	/**
	 * Check if the command-line arguments are valid for Validators.
	 * 
	 * @param arguments
	 * @param mandatoryAttributes
	 * @return
	 */
	public static boolean hasValidArgumentsValidators(Map<String, String> arguments, List<String> mandatoryAttributes) {
		String errorMessage = checkMandatoryAttributes(arguments, mandatoryAttributes);
		if (errorMessage.length() == 0) {
			if (Integer.parseInt(arguments.get(Constants.TOTAL)) > 0) {
				// do nothing
			} else {
				errorMessage = Constants.INCONSISTENT_TOTAL_ERROR;
			}
		}
		if (errorMessage.length() > 0) {
			System.out.println(String.format(Constants.MANDATORY_PARAMETERS_ERROR, errorMessage));
		}
		return (errorMessage.length() == 0);
	}

	/**
	 * Check mandatory attributes and capture missing ones.
	 * 
	 * @param arguments
	 * @param mandatoryAttributes
	 * @return
	 */
	public  static String checkMandatoryAttributes(Map<String, String> arguments, List<String> mandatoryAttributes) {
		StringBuilder errorMessage = new StringBuilder();
		if (arguments != null
				&& arguments.size() > 0) {
			for (String mandatoryAttribute : mandatoryAttributes) {
				if (arguments.get(mandatoryAttribute) != null
						&& arguments.get(mandatoryAttribute).length() > 0) {
					// do nothing
				} else {
					errorMessage.append(mandatoryAttribute).append(Constants.SPACE);
				}
			}
		} else {
			errorMessage.append(mandatoryAttributes);
		}
		return errorMessage.toString().trim();
	}

	/**
	 * Print results as per status.
	 * 
	 * @param status
	 * @param success
	 * @param failure
	 */
	public static void printStatus(boolean status, String success, String failure) {
		System.out.println(Constants.LINE);
		if (status) {
			System.out.println(success);				
		} else {
			System.out.println(failure);
		}
		System.out.println(Constants.LINE);
	}

	/**
	 * Initialize parameters.
	 * 
	 * @param args
	 */
	public static void init(String[] args, Map<String, String> arguments) {
		arguments.put(Constants.PRIMARY_ORDER, Constants.DEFAULT_PRIMARY_KEY_ORDER);
		arguments.put(Constants.SECONDARY_ORDER, Constants.DEFAULT_SECONDARY_KEY_ORDER);
		arguments.putAll(SmartSortUtilities.parseArguments(args));
	}
}
