package main.java.b669.mongodb;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import main.java.b669.mets.METSParser.VolumeRecord;
import main.java.b669.utility.DBHelper;

import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

/**
 * Handles the MongoDB connection, insertion of metadata and volume records and
 * query evaluation
 * 
 * @author harsh
 * 
 */
public class MongoDBHelper implements DBHelper {

	// Mongo config info
	private static final String MONGO_HOST = "localhost";
	private static final int MONGO_PORT = 27017;

	private static final String MONGO_DATABASE = "B669_P1"; // Mongo database

	// Mongo collections
	private static final String MONGO_COLLECTION_VOLUME_METADATA = "volumemetada";
	private static final String MONGO_COLLECTION_VOLUME_RECORDS = "volumerecords";

	protected DBCollection volumeMetadataCollection;
	protected DBCollection volumeRecordsCollection;
	protected DB database;
	protected Mongo mongo;

	protected Logger logger = Logger.getLogger(MongoDBHelper.class);

	public MongoDBHelper() {

		try {

			mongo = new Mongo(MONGO_HOST, MONGO_PORT);
			database = mongo.getDB(MONGO_DATABASE);
			volumeMetadataCollection = database.getCollection(MONGO_COLLECTION_VOLUME_METADATA);
			volumeRecordsCollection = database.getCollection(MONGO_COLLECTION_VOLUME_RECORDS);

		} catch (UnknownHostException e) {
			logger.debug(e.getMessage());
		}
	}

	/**
	 * Inserts meta-data into collection
	 */
	@Override
	public void insertVolume(VolumeRecord volumeRecord) {

		String volumeName = volumeRecord.getVolumeID();
		Set<String> pageIDs = volumeRecord.getPageIDSet();
		long numPagesInVolumeRecord = pageIDs.size();

		long volumeSize = 0;
		long pageByteCount;

		/* get the total page byte count for a volume */
		for (String s : pageIDs) {

			pageByteCount = volumeRecord.getPageRecordByID(s).getByteCount();
			volumeSize += pageByteCount;
		}

		String jsonVolume = "{'volumename' :'" + volumeName + "', 'pagecount' : "
				+ numPagesInVolumeRecord + ",'volumesize' : " + volumeSize + "}";

		DBObject dbObject = (DBObject) JSON.parse(jsonVolume);

		volumeMetadataCollection.insert(dbObject);
	}

	@Override
	public void insertVolume(String volumeName, Map<String, String> pageContentMap) {

		BasicDBObjectBuilder obj = BasicDBObjectBuilder.start();

		obj.add("volumename", volumeName);
		obj.add("pagerecords", pageContentMap);

		volumeRecordsCollection.insert(obj.get());
	}

	/**
	 * Find out average time to read a volume
	 */
	public double readVolumeQuery() {

		long numVolumes = 0;
		long startTime = System.nanoTime();
		DBCursor cursor = database.getCollection(MONGO_COLLECTION_VOLUME_RECORDS).find().limit(100);

		while (cursor.hasNext()) {

			DBObject obj = cursor.next();
			obj.get("volumename");
			obj.get("pagerecords");
			numVolumes++;
		}

		long endTime = System.nanoTime();

		double averageTime = ((double) ((endTime - startTime) * 1.0) / ((double) (1000000.0 * numVolumes)));

		logger.debug("time taken to read a volume: " + averageTime + "ms");
		//logger.debug("volumes read: " + numVolumes);

		return averageTime;

	}

	/**
	 * Finds the volume with max number of pages
	 */
	@Override
	public void evaluateQueryForMaxNumberOfPagesInVolume() {

		long startTime = System.nanoTime();

		List<DBObject> list = database.getCollection(MONGO_COLLECTION_VOLUME_METADATA).find()
				.sort(new BasicDBObject("pagecount", -1)).limit(1).toArray();

		if (list != null)
			logger.debug("max page count: " + (Long) list.get(0).get("pagecount") + ", vol_id: "
					+ list.get(0).get("volumename"));

		long endTime = System.nanoTime(); // end timer

		logger.debug("total time for query eval: " + (double) ((endTime - startTime) * 1.0)
				/ 1000000.0 + "ms");

	}

	/**
	 * Find volume with max size
	 */
	@Override
	public void evaluateQueryForMaxSizeVolume() {

		long startTime = System.nanoTime(); // start time

		List<DBObject> list = database.getCollection(MONGO_COLLECTION_VOLUME_METADATA).find()
				.sort(new BasicDBObject("volumesize", -1)).limit(1).toArray();

		if (list != null)
			logger.debug("max size: " + (Long) list.get(0).get("volumesize") + ", vol_id: "
					+ list.get(0).get("volumename"));

		long endTime = System.nanoTime(); // end timer

		logger.debug("total time for query eval: " + (double) ((endTime - startTime) * 1.0)
				/ 1000000.0 + "ms");
	}

	/**
	 * Finds average size of volume
	 */
	@Override
	public double evaluateQueryForAverageVolumeSize() {

		long totalVolumes = 0;
		long totalSize = 0;
		long endTime;

		long startTime = System.nanoTime(); // start time

		DBCursor cursor = volumeMetadataCollection.find();

		if (cursor == null)
			return Double.MIN_VALUE;

		while (cursor.hasNext()) {

			DBObject dbObj = cursor.next();

			if (dbObj != null) {

				Object obj = dbObj.get("volumesize");

				if (obj != null) {

					totalSize += (Long) obj;
					totalVolumes++;
				}
			}
		}

		double averageSize = (double) ((double) (totalSize * 1.0) / totalVolumes);
		logger.debug("averageSize of volume: " + averageSize);

		endTime = System.nanoTime(); // end timer

		logger.debug("total time for query eval: "
				+ ((double) ((double) ((endTime - startTime) * 1.0)) / 1000000.0) + "ms");

		return averageSize;
	}
}