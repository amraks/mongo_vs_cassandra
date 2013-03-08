package main.java.b669.cassandra;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import main.java.b669.mets.METSParser.VolumeRecord;
import main.java.b669.utility.DBHelper;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.SuperSlice;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SuperSliceQuery;

import org.apache.log4j.Logger;

/**
 * Manages the Cassandra connection handling, inserting data
 * 
 * @author harsh
 * 
 */
public class CassandraHelper implements DBHelper {

	protected Mutator<String> mutator;
	protected Cluster cluster;
	public Keyspace keySpace;

	public static StringSerializer stringSerializer = StringSerializer.get();
	public static LongSerializer longSerializer = LongSerializer.get();

	public static final String CASSANDRA_HOST_PORT_CONFIG = "localhost:9160";
	public static final String CASSANDRA_KEYSPACE = "B669_P1";
	public static final String CASSANDRA_CLUSTER = "Test Cluster";

	public static final String PAGE_VOLUME_SFAMILY = "PageVolumeSFamily";
	public static final String SIZE_VOULME_SFAMILY = "SizeVolumeSFamily";
	public static final String VOLUME_RECORD_CFAMILY = "VolumeRecordsCFamily";

	public int volumesProcessed = 0;
	private static Logger logger = Logger.getLogger(CassandraHelper.class);

	public static final String uniqueRowKey = "TheLonlyRow"; // the only row
																// key!

	public CassandraHelper() {

		this.cluster = HFactory.getOrCreateCluster(CASSANDRA_CLUSTER, CASSANDRA_HOST_PORT_CONFIG);
		this.keySpace = HFactory.createKeyspace(CASSANDRA_KEYSPACE, cluster);
		this.mutator = HFactory.createMutator(keySpace, stringSerializer);
	}

	/**
	 * Inserts Volume meta-data
	 * 
	 * @param volumeRecord
	 */
	@Override
	public void insertVolume(VolumeRecord volumeRecord) {

		String volumeName = volumeRecord.getVolumeID();
		Set<String> pageIDs = volumeRecord.getPageIDSet();
		long numPagesInVolumeRecord = pageIDs.size();

		/*
		 * insert data for PageVolumeSFamily PAGE_VOLUME_SFAMILY is column
		 * family of type super, where there is single row, page numbers are the
		 * super columns and volume id's are the columns
		 */

		mutator.insert(uniqueRowKey, PAGE_VOLUME_SFAMILY, HFactory.createSuperColumn(
				numPagesInVolumeRecord,
				Arrays.asList(HFactory.createStringColumn(volumeName, "NOUSE")), longSerializer,
				stringSerializer, stringSerializer));

		volumesProcessed++; // for debug purpose

		long volumeSize = 0;
		long pageByteCount;

		for (String s : pageIDs) { // get total page byte count

			pageByteCount = volumeRecord.getPageRecordByID(s).getByteCount();
			volumeSize += pageByteCount;
		}

		/* insert data for SizeVolumeSFamily */

		mutator.insert(uniqueRowKey, SIZE_VOULME_SFAMILY, HFactory.createSuperColumn(volumeSize,
				Arrays.asList(HFactory.createStringColumn(volumeName, "NOUSE")), longSerializer,
				stringSerializer, stringSerializer));
	}

	/**
	 * Inserts Volume Zip data
	 * 
	 * @param volumeRecord
	 * @param pageContentMap
	 */

	@Override
	public void insertVolume(String volumeName, Map<String, String> pageContentMap) {

		Set<Entry<String, String>> set = pageContentMap.entrySet();

		for (Entry<String, String> e : set) {

			mutator.insert(volumeName, VOLUME_RECORD_CFAMILY,
					HFactory.createStringColumn(e.getKey(), e.getValue()));
		}

	}

	public Keyspace getKeySpace() {
		return keySpace;
	}

	public void evaluateQueryForMaxNumberOfPagesInVolume() {
		// through cli
	}

	public void evaluateQueryForMaxSizeVolume() {
		// through cli
	}

	/**
	 * Gets the average size for the volume
	 */
	public double evaluateQueryForAverageVolumeSize() {

		long totalSize = 0;
		long numVolumes = 0;

		SuperSliceQuery<String, Long, String, String> query = HFactory.createSuperSliceQuery(
				keySpace, stringSerializer, longSerializer, stringSerializer, stringSerializer);

		query.setColumnFamily(SIZE_VOULME_SFAMILY);
		query.setKey(uniqueRowKey);
		query.setRange(Long.MAX_VALUE, 0L, false, Integer.MAX_VALUE); // already
																		// reversed
		long localVolumes;
		long startTime = System.nanoTime();

		QueryResult<SuperSlice<Long, String, String>> result = query.execute();
		SuperSlice<Long, String, String> slice = result.get();
		List<HSuperColumn<Long, String, String>> superColumns = slice.getSuperColumns();

		for (HSuperColumn<Long, String, String> sc : superColumns) {

			Object o = sc.getName();

			if (o instanceof Long) {

				// more than 1 volume can have same size
				localVolumes = sc.getColumns().size();
				totalSize += ((Long) o) * localVolumes;
				numVolumes += localVolumes;

			}
		}

		long endTime = System.nanoTime();

		logger.debug("time taken to find average volume: " + ((double) (endTime - startTime) * 1.0) / (double) (1000000.0)
				+ "ms");
		logger.debug("averageSize: " + (double) (totalSize * 1.0) / (double) (numVolumes * 1.0));

		return (double) (totalSize * 1.0) / (double) (numVolumes * 1.0);
	}

	/**
	 * 
	 */
	@Override
	public double readVolumeQuery() {

		RangeSlicesQuery<String, String, String> rangeSlicesQuery = HFactory
				.createRangeSlicesQuery(keySpace, stringSerializer, stringSerializer,
						stringSerializer);

		rangeSlicesQuery.setColumnFamily(VOLUME_RECORD_CFAMILY);
		rangeSlicesQuery.setKeys("", "");
		rangeSlicesQuery.setRange("", "", false, Integer.MAX_VALUE); // gets all
																		// columns
		rangeSlicesQuery.setRowCount(100); // limit

		long startTime = System.nanoTime();

		QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
		OrderedRows<String, String, String> orderedRows = result.get();

		for (Row<String, String, String> r : orderedRows) {

			ColumnSlice<String, String> slice = r.getColumnSlice();
			List<HColumn<String, String>> listColumns = slice.getColumns();

			for (HColumn<String, String> cl : listColumns) {
				//logger.debug("vol_id: " + r.getKey() + ", page_id: " + cl.getName());
			}
		}

		long endTime = System.nanoTime();
		long countOfRows = orderedRows.getCount();

		logger.debug("time taken to read volume: " + ((double) (endTime - startTime) * 1.0)
				/ (double) (countOfRows * 1000000.0) + "ms");

		return ((double) (endTime - startTime) * 1.0) / (double) (countOfRows * 1000000.0);
	}
}