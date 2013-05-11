package main.java.b669.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.List;

import org.apache.log4j.Logger;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MapReduceOutput;
import com.mongodb.Mongo;

public class MongoDBMapReduce {

	private static final String MONGO_DATABASE = "B669_P1";
	private static final String MONGO_COLLECTION_VOLUME_RECORDS = "volumerecords";
	private static final String MONGO_HOST = "localhost";
	private static final int MONGO_PORT = 27017;
	private static Mongo mongo;
	private static DBCollection collection;
	private static DB database;
	private static String results1 = "results1";
	private static String results2 = "results2";
	private static String results3 = "results3";
	private static double totalVolumes = 1644;

	private static final Logger log = Logger.getLogger(MongoDBMapReduce.class);

	public static void handle() throws Exception {

		/* ============= Connection handling ==================== */

		mongo = new Mongo(MONGO_HOST, MONGO_PORT);
		database = mongo.getDB(MONGO_DATABASE);
		collection = database.getCollection(MONGO_COLLECTION_VOLUME_RECORDS);

		String map;
		String reduce;
		MapReduceOutput out;

		/* ============== Phase 1 ========================== */
		map = readFromFile("map1.js");
		reduce = readFromFile("reduce1.js");

		out = collection.mapReduce(map, reduce, results1, null);
		// printResults(out);

		/* ============== Phase 2 ========================== */

		collection = database.getCollection(results1);

		map = readFromFile("map2.js");
		reduce = readFromFile("reduce2.js");

		out = collection.mapReduce(map, reduce, results2, null);
		// printResults(out);

		/* ============== Phase 3 ========================== */

		collection = database.getCollection(results2);

		map = readFromFile("map3.js");
		reduce = readFromFile("reduce3.js");

		out = collection.mapReduce(map, reduce, results3, null);
		// printResults(out);

		/* ==================================================== */

		calculateTFIDF();
	}

	private static void calculateTFIDF() {
		collection = database.getCollection(results3);
		DBCursor cursor = collection.find();
		DBObject dbobj;
		List<DBObject> list;
		double tfidf;
		String word;
		String volumeName;
		double count;
		double max_count;
		double numVolumesAppeared;

		while (cursor.hasNext()) {

			dbobj = cursor.next();

			list = (List<DBObject>) (((DBObject) dbobj.get("value")).get("arr"));
			word = (String) (((DBObject) dbobj.get("_id")).get("word"));
			numVolumesAppeared = ((Double) (((DBObject) dbobj.get("value"))
					.get("num_volumes_appeared"))).doubleValue();

			if (list == null) {
				log.debug("NULL list");
				break;
			}

			for (DBObject d : list) {
				volumeName = (String) d.get("volumename");
				count = ((Double) d.get("count")).doubleValue();
				max_count = ((Double) d.get("max_count")).doubleValue();
				tfidf = (count / max_count) * (Math.log((totalVolumes / numVolumesAppeared)));
				log.debug("word:" + word + ", vol_id:" + volumeName + ", tfidf:" + tfidf);
			}
		}
	}

	private static void printResults(MapReduceOutput out) {
		for (DBObject o : out.results())
			log.debug(o.toString());
	}

	private static String readFromFile(String file) {
		String content = new String();
		BufferedReader reader = null;

		try {

			reader = new BufferedReader(new FileReader(file));
			String line = new String();

			while ((line = reader.readLine()) != null) {
				content += line + System.getProperty("line.separator");
			}
			reader.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		return content;
	}
}
