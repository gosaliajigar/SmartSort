package edu.itu.csc.validators;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.itu.csc.common.SmartSortUtilities;
import edu.itu.csc.constants.Constants;

/**
 * SmartSort Post Validator to validate if sorted file is sorted or not.
 * 
 * @author "Jigar Gosalia"
 * 
 */
public class SmartSortPostValidator extends Configured implements Tool {

	public static Map<String, String> arguments = new HashMap<String, String>();

	public static final List<String> mandatoryFields = new LinkedList<String>();

	static {
		mandatoryFields.add(Constants.INPUT);
		mandatoryFields.add(Constants.OUTPUT);
		mandatoryFields.add(Constants.TOTAL);
		mandatoryFields.add(Constants.DELIMITER);
	}

	/* Mapper Class */
	public static class PostValidatorMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		String previousLine = null;

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (value != null
					&& value.toString().length() > 0
					&& value.toString().contains(arguments.get(Constants.DELIMITER))) {
				String currentLine = value.toString();
				fieldCount(currentLine, context);
				if (previousLine != null
						&& currentLine != null) {
					compare(previousLine, currentLine, context);
				}
				previousLine = currentLine;
			} else {
				StringBuilder data = new StringBuilder();
				String fileName = ((FileSplit)(context).getInputSplit()).getPath().getName();
				data.append(Constants.POST_VALIDATION_FAILED)
					.append(String.format(Constants.INVALID_DATA_ERROR, fileName, arguments.get(Constants.DELIMITER), ((value != null) ? value.toString() : value)));
				throw new InterruptedIOException(data.toString());
			}
		}

		/**
		 * Check field count for current line.
		 * 
		 * @param currentLine
		 * @param context
		 * @throws InterruptedIOException
		 */
		private void fieldCount(String currentLine, Context context) throws InterruptedIOException {
			String[] fields = currentLine.split(arguments.get(Constants.DELIMITER));
			if (fields != null
					&& fields.length > 0
					&& fields.length == Integer.parseInt(arguments.get(Constants.TOTAL))) {
				// do nothing
			} else {
				StringBuilder data = new StringBuilder();
				String fileName = ((FileSplit)(context).getInputSplit()).getPath().getName();
				data.append(Constants.POST_VALIDATION_FAILED)
					.append(String.format(Constants.DATA_INCONSISTENT_ERROR, fileName, arguments.get(Constants.DELIMITER), currentLine));
				throw new InterruptedIOException(data.toString());
			}
		}

		/**
		 * Compare previous and current lines along with their sorting order of the primary and secondary key.
		 * 
		 * @param previousLine
		 * @param currentLine
		 * @param context
		 * @throws InterruptedIOException
		 */
		private void compare(String previousLine, String currentLine, Context context) throws InterruptedIOException {
			boolean isValid = false;
			String[] previousFields = previousLine.split(arguments.get(Constants.DELIMITER));
			String[] currentFields = currentLine.split(arguments.get(Constants.DELIMITER));

			if (previousFields != null
					&& previousFields.length >= 2
					&& currentFields != null
					&& currentFields.length >= 2) {
				if (Constants.ASC.equals(arguments.get(Constants.PRIMARY_ORDER))
						&& Constants.ASC.equals(arguments.get(Constants.SECONDARY_ORDER))) {
					isValid = ((previousFields[0].compareTo(currentFields[0]) < 0)
							|| ((previousFields[0].compareTo(currentFields[0]) == 0)
									&& (previousFields[1].compareTo(currentFields[1]) <= 0)));
				} else if (Constants.ASC.equals(arguments.get(Constants.PRIMARY_ORDER))
						&& Constants.DESC.equals(arguments.get(Constants.SECONDARY_ORDER))) {
					isValid = ((previousFields[0].compareTo(currentFields[0]) < 0)
							|| ((previousFields[0].compareTo(currentFields[0]) == 0)
									&& (previousFields[1].compareTo(currentFields[1]) >= 0)));
				} else if (Constants.DESC.equals(arguments.get(Constants.PRIMARY_ORDER))
						&& Constants.ASC.equals(arguments.get(Constants.SECONDARY_ORDER))) {
					isValid = ((previousFields[0].compareTo(currentFields[0]) > 0)
							|| ((previousFields[0].compareTo(currentFields[0]) == 0)
									&& (previousFields[1].compareTo(currentFields[1]) <= 0)));
				} else if (Constants.DESC.equals(arguments.get(Constants.PRIMARY_ORDER))
						&& Constants.DESC.equals(arguments.get(Constants.SECONDARY_ORDER))) {
					isValid = ((previousFields[0].compareTo(currentFields[0]) > 0)
							|| ((previousFields[0].compareTo(currentFields[0]) == 0)
									&& (previousFields[1].compareTo(currentFields[1]) >= 0)));
				}
			}

			if (!isValid) {
				StringBuilder data = new StringBuilder();
				String fileName = ((FileSplit)(context).getInputSplit()).getPath().getName();
				data.append(Constants.POST_VALIDATION_FAILED)
					.append(String.format(Constants.COMPARISON_FAILED_ERROR, fileName, arguments.get(Constants.PRIMARY_ORDER), arguments.get(Constants.SECONDARY_ORDER), previousLine, currentLine));
				throw new InterruptedIOException(data.toString());
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		boolean status = false;

		SmartSortUtilities.init(args, arguments);

		if (SmartSortUtilities.hasValidArgumentsPostSort(arguments, mandatoryFields)) {
			SmartSortUtilities.printArguments(arguments);

			Job job = new Job(getConf());
			job.setJobName(Constants.SMART_SORT_POST_VALIDATOR_JOB);

			job.setJarByClass(SmartSortPostValidator.class);

			FileInputFormat.addInputPath(job, new Path(arguments.get(Constants.INPUT)));
			FileOutputFormat.setOutputPath(job, new Path(arguments.get(Constants.OUTPUT)));

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(PostValidatorMapper.class);
			// no reducers needed as validation is part of mapper itself
			job.setNumReduceTasks(0);

			status = job.waitForCompletion(true);

			SmartSortUtilities.printStatus(status, Constants.POST_VALIDATION_SUCCESSFUL, Constants.POST_VALIDATION_FAILED);
		} else {
			SmartSortUtilities.printUsage(Constants.SMART_SORT_POST_VALIDATOR_USAGE);
		}

		SmartSortUtilities.printArguments(arguments);

		return status ? 0 : 1;
	}

	/**
	 * Main Method.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// ToolRunner handling command-line options.
		int result = ToolRunner.run(new Configuration(), new SmartSortPostValidator(), args);
		System.exit(result);
	}
}
