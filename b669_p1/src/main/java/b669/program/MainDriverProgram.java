package main.java.b669.program;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import main.java.b669.cassandra.CassandraHelper;
import main.java.b669.mongodb.MongoDBHelper;
import main.java.b669.utility.Constants;
import main.java.b669.utility.DBHelper;
import main.java.b669.utility.DirectoryRecurser;

import org.apache.log4j.Logger;

/**
 * Main program, execution begins here
 * 
 * @author harsh
 * 
 */
public class MainDriverProgram {

	private static final String LOG_PROPERTIES_FILE = "log4j.properties";
	protected static Logger logger = Logger.getLogger(MainDriverProgram.class);

	public static void workWithCassandra(DirectoryRecurser directoryRecurser, File file) {

		DBHelper dbHelper = new CassandraHelper();

		directoryRecurser.setDbHelper(dbHelper);
		logger.debug("Cassandra insertion operation starting.....");
		directoryRecurser.searchDirectory(file);
		logger.debug("Cassandra insertion operation finished!");

		dbHelper.evaluateQueryForMaxNumberOfPagesInVolume(); // query 1
		dbHelper.evaluateQueryForMaxSizeVolume(); // query 2
		dbHelper.evaluateQueryForAverageVolumeSize(); // query 3

		// time to insert a volume
		logger.debug("time taken(in ms) to insert volume in cassandra: "
				+ (double) (((double) (directoryRecurser.timeTakenToInsertVolume * 1.0)) / directoryRecurser.totalVolumesInserted));

		dbHelper.readVolumeQuery(); // time to read a volume

		directoryRecurser.timeTakenToInsertVolume = 0; // reset the time values
		directoryRecurser.totalVolumesInserted = 0;
	}

	public static void workWithMongoDB(DirectoryRecurser directoryRecurser, File file) {

		DBHelper dbHelper = new MongoDBHelper();

		directoryRecurser.setDbHelper(dbHelper);
		logger.debug("MongoDB insertion operation statring....");
		directoryRecurser.searchDirectory(file); // recurse on the directories
		logger.debug("MongoDB insertion operation finished!");

		dbHelper.evaluateQueryForMaxNumberOfPagesInVolume(); // query 1
		dbHelper.evaluateQueryForMaxSizeVolume(); // query 2
		dbHelper.evaluateQueryForAverageVolumeSize(); // query 3

		// time taken to insert a single volume
		logger.debug("time taken(in ms) to insert volume in mongodb: "
				+ (double) (((double) (directoryRecurser.timeTakenToInsertVolume * 1.0)) / directoryRecurser.totalVolumesInserted));

		dbHelper.readVolumeQuery(); // calculate time to read a volume

		directoryRecurser.timeTakenToInsertVolume = 0; // reset the time values
		directoryRecurser.totalVolumesInserted = 0;
	}

	public static void main(String[] args) {

		Properties logProperties = new Properties();

		try {
			logProperties.load(new FileInputStream(LOG_PROPERTIES_FILE));
		} catch (IOException e) {
			// do nothing
		}

		DirectoryRecurser directoryRecurser = new DirectoryRecurser();
		File file = new File(Constants.ROOT_DIRECTORY);
		workWithCassandra(directoryRecurser, file);
		workWithMongoDB(directoryRecurser, file);
	}
}