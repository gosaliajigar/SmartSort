package edu.itu.csc.driver;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.itu.csc.common.SmartSortUtilities;
import edu.itu.csc.comparators.GroupingComparator;
import edu.itu.csc.constants.Constants;
import edu.itu.csc.key.CompositeKeyWritable;

/**
 * Smart Sort to demonstrate Secondary Sort by sorting data using
 * MapReduce.<br>
 * &nbsp - capability to sort data using 2 keys<br>
 * &nbsp - capability to sort data in any order asc or desc<br><br>
 * 
 *<table>
 * <tr>
 *  <th>Primary</th>
 *  <th>Secondary</th> 
 * </tr>
 * <tr>
 *  <td>asc</td>
 *  <td>asc</td> 
 * </tr>
 * <tr>
 *  <td>asc</td>
 *  <td>desc</td>
 * </tr>
 * <tr>
 *  <td>desc</td>
 *  <td>asc</td>
 * </tr>
 * <tr>
 *  <td>desc</td>
 *  <td>desc</td>
 * </tr>
 *</table>
 * 
 * 
 * @author "Jigar Gosalia"
 * 
 */
public class SmartSortDriver extends Configured implements Tool {

	public static int totalkeys = -1;

	public static int primaryKey = -1;

	public static int secondaryKey = -1;

	public static String delimiter = Constants.EMPTY;

	public static Map<String, String> arguments = new HashMap<String, String>();;

	public static final List<String> mandatoryFields = new LinkedList<String>();

	static {
		mandatoryFields.add(Constants.INPUT);
		mandatoryFields.add(Constants.OUTPUT);
		mandatoryFields.add(Constants.PRIMARY);
		mandatoryFields.add(Constants.SECONDARY);
		mandatoryFields.add(Constants.TOTAL);
		mandatoryFields.add(Constants.DELIMITER);
	}

	/* Mapper Class */
	public static class SmartSortMapper extends
			Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			if (value != null && value.toString() != null
					&& value.toString().length() > 0) {
				String fields[] = value.toString().split(delimiter);
				if (fields.length > 0 && fields.length == totalkeys) {
					CompositeKeyWritable compositeKey = constructCompositeKey(fields);
					context.write(compositeKey, NullWritable.get());
				} else {
					StringBuilder data = new StringBuilder();
					String fileName = ((FileSplit)(context).getInputSplit()).getPath().getName();
					data.append(String.format(Constants.DATA_INCONSISTENT_ERROR, fileName, arguments.get(Constants.DELIMITER), value.toString()));
					throw new InterruptedIOException(data.toString());
				}
			}
		}

		/**
		 * 
		 */
		private static CompositeKeyWritable constructCompositeKey(
				String[] fields) {
			CompositeKeyWritable compositeKey = new CompositeKeyWritable();
			compositeKey.setPrimaryKey(fields[primaryKey]);
			compositeKey.setDelimiter(delimiter);
			StringBuilder secondaryKeyValue = new StringBuilder();
			secondaryKeyValue.append(fields[secondaryKey]).append(delimiter);
			for (int index = 0; index < fields.length; index++) {
				if (index != primaryKey && index != secondaryKey) {
					secondaryKeyValue.append(fields[index]).append(delimiter);
				}
			}
			if (secondaryKeyValue.toString().length() > 0
					&& secondaryKeyValue.toString().charAt(
							secondaryKeyValue.length() - 1) == delimiter
							.charAt(0)) {
				secondaryKeyValue.deleteCharAt(secondaryKeyValue.toString()
						.length() - 1);
			}
			compositeKey.setSecondaryKey(secondaryKeyValue.toString());
			return compositeKey;
		}
	}

	/* Reducer Class */
	public static class SmartSortReducer extends
			Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable> {

		@SuppressWarnings("unused")
		@Override
		public void reduce(CompositeKeyWritable key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			for (NullWritable value: values) {
				context.write(key, NullWritable.get());
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		boolean status = false;

		SmartSortUtilities.init(args, arguments);

		if (SmartSortUtilities.hasValidArgumentsSort(arguments, mandatoryFields)) {
			populateGlobalVariables();

			SmartSortUtilities.printArguments(arguments);

			Job job = new Job(getConf());
			job.setJobName(Constants.SMART_SORT_DRIVER_JOB);

			job.setJarByClass(SmartSortDriver.class);

			FileInputFormat.setInputPaths(job, new Path(arguments.get(Constants.INPUT)));
			FileOutputFormat.setOutputPath(job, new Path(arguments.get(Constants.OUTPUT)));

			job.setMapperClass(SmartSortMapper.class);
			job.setMapOutputKeyClass(CompositeKeyWritable.class);
			job.setMapOutputValueClass(NullWritable.class);

			job.setPartitionerClass(SmartSortUtilities.getPartitioner(arguments));
			job.setSortComparatorClass(SmartSortUtilities.getSortComparator(arguments));
			job.setGroupingComparatorClass(GroupingComparator.class);

			job.setReducerClass(SmartSortReducer.class);
			job.setOutputKeyClass(CompositeKeyWritable.class);
			job.setOutputValueClass(NullWritable.class);
			job.setNumReduceTasks(SmartSortUtilities.getReducers(arguments));

			status = job.waitForCompletion(true);
		} else {
			SmartSortUtilities.printUsage(Constants.SMART_SORT_USAGE);
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
		int result = ToolRunner.run(new Configuration(), new SmartSortDriver(), args);
		System.exit(result);
	}

	/**
	 * Populate global parameters.
	 * 
	 */
	private static void populateGlobalVariables() {
		primaryKey = Integer.parseInt(arguments.get(Constants.PRIMARY));
		secondaryKey = Integer.parseInt(arguments.get(Constants.SECONDARY));
		totalkeys = Integer.parseInt(arguments.get(Constants.TOTAL));
		delimiter = arguments.get(Constants.DELIMITER);
	}
}