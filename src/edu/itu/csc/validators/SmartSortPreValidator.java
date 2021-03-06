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
 * SmartSort Pre Validator to validate if sorted file is sorted or not.
 * 
 * @author "Jigar Gosalia"
 * 
 */
public class SmartSortPreValidator extends Configured implements Tool {

	public static Map<String, String> arguments = new HashMap<String, String>();

	public static final List<String> mandatoryFields = new LinkedList<String>();

	static {
		mandatoryFields.add(Constants.INPUT);
		mandatoryFields.add(Constants.OUTPUT);
		mandatoryFields.add(Constants.TOTAL);
		mandatoryFields.add(Constants.DELIMITER);
	}

	/* Mapper Class */
	public static class PreValidatorMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (value != null
					&& value.toString().length() > 0
					&& value.toString().contains(arguments.get(Constants.DELIMITER))) {
				String[] fields = value.toString().split(arguments.get(Constants.DELIMITER));
				if (fields != null && fields.length > 0 && fields.length == Integer.parseInt(arguments.get(Constants.TOTAL))) {
					// do nothing
				} else {
					StringBuilder data = new StringBuilder();
					String fileName = ((FileSplit)(context).getInputSplit()).getPath().getName();
					data.append(Constants.PRE_VALIDATION_FAILED)
						.append(String.format(Constants.DATA_INCONSISTENT_ERROR, fileName, arguments.get(Constants.DELIMITER), value.toString()));
					throw new InterruptedIOException(data.toString());
				}
			} else {
				StringBuilder data = new StringBuilder();
				String fileName = ((FileSplit)(context).getInputSplit()).getPath().getName();
				data.append(Constants.PRE_VALIDATION_FAILED)
					.append(String.format(Constants.INVALID_DATA_ERROR, fileName, arguments.get(Constants.DELIMITER), ((value != null) ? value.toString() : value)));
				throw new InterruptedIOException(data.toString());
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		boolean status = false;

		SmartSortUtilities.init(args, arguments);

		if (SmartSortUtilities.hasValidArgumentsPreSort(arguments, mandatoryFields)) {
			SmartSortUtilities.printArguments(arguments);

			Job job = new Job(getConf());
			job.setJobName(Constants.SMART_SORT_PRE_VALIDATOR_JOB);

			job.setJarByClass(SmartSortPreValidator.class);

			FileInputFormat.addInputPath(job, new Path(arguments.get(Constants.INPUT)));
			FileOutputFormat.setOutputPath(job, new Path(arguments.get(Constants.OUTPUT)));

			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(PreValidatorMapper.class);
			// no reducers needed as validation is part of mapper itself
			job.setNumReduceTasks(0);

			status = job.waitForCompletion(true);

			SmartSortUtilities.printStatus(status, Constants.PRE_VALIDATION_SUCCESSFUL, Constants.PRE_VALIDATION_FAILED);
		} else {
			SmartSortUtilities.printUsage(Constants.SMART_SORT_PRE_VALIDATOR_USAGE);
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
		int result = ToolRunner.run(new Configuration(), new SmartSortPreValidator(), args);
		System.exit(result);
	}
}