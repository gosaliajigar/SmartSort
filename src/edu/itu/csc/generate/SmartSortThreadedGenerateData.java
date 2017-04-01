package edu.itu.csc.generate;

import java.io.File;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

import edu.itu.csc.constants.Constants;

/**
 * Generate Data using threads.
 *
 */
public class SmartSortThreadedGenerateData implements Runnable {

	private Thread thread;

	private FileWriter writer;

	private String fileName;

	private final Random random = new Random();

	private static final String lexicon = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

	private static final String[] cityArray = { "Adelanto", "Antioch",
			"Bakersfield", "Chino", "Chico", "Gilroy" };

	private static final String[] stateArray = { "CA", "AL", "AK", "AR", "CO",
			"CT", "DE", "DC", "WI", "WY" };

	public SmartSortThreadedGenerateData(FileWriter fileWriter, String fileName) {
		writer = fileWriter;
		this.fileName = fileName;
	}

	@Override
	public void run() {
		new Timer().schedule(new TimerTask() {
			@Override
			public void run() {
				try {
					writer.flush();
				} catch (Exception exception) {
					exception.printStackTrace();
				}
			}
		}, 10000);

		while (true) {
			// generate random data
			String record = getRandomRecord();

			// exit if file size is reached
			isMaxFileSize(fileName);

			// write record to file
			writeRecord(record);
		}
	}

	private void writeRecord(String record) {
		try {
			writer.write(record);
			writer.write('\n');
		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}

	private void isMaxFileSize(String fileName2) {
		if (new File(fileName).length() > new Long(2048000000)) {
			StringBuilder data = new StringBuilder();
			data.append("Generated file ")
					.append(fileName)
					.append(" at ")
					.append(new SimpleDateFormat("yyyyMMdd_HHmmss")
							.format(Calendar.getInstance().getTime()));
			System.out.println(data.toString());

			try {
				writer.close();
			} catch (Exception exception) {
				exception.printStackTrace();
			}
			return;
		}
	}

	private String getRandomRecord() {
		StringBuilder randomData = new StringBuilder();
		randomData.append(getRandomFirstName()).append(Constants.COMMA)
				.append(getRandomLastName()).append(Constants.COMMA)
				.append(getRandomCity()).append(Constants.COMMA)
				.append(getRandomState()).append(Constants.COMMA)
				.append(getRandomZip()).append(Constants.COMMA)
				.append(getRandomCC()).append(Constants.COMMA)
				.append(getRandomIpAddress());
		return randomData.toString();
	}

	private String getRandomString() {
		StringBuilder builder = new StringBuilder();
		while (builder.toString().length() == 0) {
			int length = random.nextInt(5) + 5;
			for (int i = 0; i < length; i++) {
				builder.append(lexicon.charAt(random.nextInt(lexicon.length())));
			}
		}
		return builder.toString();
	}

	private int getRandomNumber(int min, int max) {
		int randomNumber = new Random().nextInt(max);
		if (randomNumber < min) {
			randomNumber += min;
		}
		return randomNumber;
	}

	private String getRandomArrayElement(String[] array) {
		return array[new Random().nextInt(array.length)];
	}

	private String getRandomFirstName() {
		return getRandomString();
	}

	private String getRandomLastName() {
		return getRandomString();
	}

	private String getRandomCity() {
		return getRandomArrayElement(cityArray);
	}

	private String getRandomState() {
		return getRandomArrayElement(stateArray);
	}

	private String getRandomCC() {
		return Integer.toString(getRandomNumber(1000, 9999))
				+ Integer.toString(getRandomNumber(1000, 9999))
				+ Integer.toString(getRandomNumber(1000, 9999))
				+ Integer.toString(getRandomNumber(1000, 9999));
	}

	private String getRandomZip() {
		return Integer.toString(getRandomNumber(10000, 99999));
	}

	private String getRandomIpAddress() {
		return Integer.toString(getRandomNumber(100, 999)) + "."
				+ Integer.toString(getRandomNumber(100, 999)) + "."
				+ Integer.toString(getRandomNumber(100, 999)) + "."
				+ Integer.toString(getRandomNumber(100, 999));
	}

	public void start() {
		if (thread == null) {
			thread = new Thread(this, fileName);
			thread.start();
		}
	}

    public static void main(String args[]) throws Exception {
    	for (int i=0; i<5; i++) {
            FileWriter writer = new FileWriter("/tmp/file" + i + ".txt", true);
            SmartSortThreadedGenerateData generate = new SmartSortThreadedGenerateData(writer, "/tmp/file" + i + ".txt");
            generate.start();
        }
    }
}
