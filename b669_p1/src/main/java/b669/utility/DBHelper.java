package main.java.b669.utility;

import java.util.Map;

import main.java.b669.mets.METSParser.VolumeRecord;

/**
 * All database helpers should implement this interface
 * 
 * @author harsh
 * 
 */
public interface DBHelper {

	public void insertVolume(VolumeRecord volumeRecord);

	public void insertVolume(String volumeName, Map<String, String> pageContentMap);

	public double evaluateQueryForAverageVolumeSize();

	public void evaluateQueryForMaxNumberOfPagesInVolume();

	public void evaluateQueryForMaxSizeVolume();

	public double readVolumeQuery();

}