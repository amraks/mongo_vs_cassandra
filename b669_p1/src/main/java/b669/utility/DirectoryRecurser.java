package main.java.b669.utility;

import gov.loc.repository.pairtree.Pairtree;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipFile;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import main.java.b669.mets.METSParser;
import main.java.b669.mets.METSParser.VolumeRecord;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * Recurses on the root directory and each of the sub-directories, calls
 * appropriate helper: cassandra or mongodb for insertion of metadata and volume
 * records
 * 
 * @author harsh
 * 
 */
public class DirectoryRecurser {

	protected XMLInputFactory xmlInputFactory;
	protected Pairtree pairTree;
	protected METSParser parser;
	protected DBHelper dbHelper;
	protected boolean parseXML; // used for debugging
	public long timeTakenToInsertVolume;
	public long totalVolumesInserted;

	protected static Logger logger = Logger.getLogger(DirectoryRecurser.class);

	public DirectoryRecurser() {

		this.xmlInputFactory = XMLInputFactory.newInstance();
		this.pairTree = new Pairtree();
		this.parser = new METSParser(null, null, xmlInputFactory);
		this.parseXML = true;
		this.timeTakenToInsertVolume = 0;
		this.totalVolumesInserted = 0;
	}

	/**
	 * Retrieves the clean volume id from the volume file name
	 * 
	 * @param currentFileName
	 * @return
	 */
	private String extractVolumeIDFromFilePath(String currentFileName) {

		if (currentFileName.endsWith("xml")) {

			String cleanedHeadlessID = currentFileName.substring(0, currentFileName.length()
					- ".mets.xml".length());

			String uncleanedHeadlessID = pairTree.uncleanId(cleanedHeadlessID);

			return Constants.NAMESPACE_PREFIX + "." + uncleanedHeadlessID;

		}

		else if (currentFileName.endsWith("zip")) {

			String cleanedHeadlessID = currentFileName.substring(0, currentFileName.length()
					- ".zip".length());

			String uncleanedHeadlessID = pairTree.uncleanId(cleanedHeadlessID);

			return Constants.NAMESPACE_PREFIX + "." + uncleanedHeadlessID;

		}

		return null;

	}

	/**
	 * Extracts each entry from zip file for a particular volume and puts it's
	 * content in map
	 * 
	 * @param currentFile
	 * @param pageContentMap
	 */
	public void getZipFileContent(File currentFile, Map<String, String> pageContentMap) {

		try {

			ZipFile zipFile = new ZipFile(currentFile);
			Enumeration<? extends ZipEntry> zipFileEntries = zipFile.entries();
			InputStream inputStream;

			while (zipFileEntries.hasMoreElements()) {

				ZipEntry zipEntry = zipFileEntries.nextElement();

				if (!zipEntry.isDirectory()) {

					String zipEntryFileName = zipEntry.getName();

					if (zipEntryFileName.endsWith("txt")) {

						inputStream = zipFile.getInputStream(zipEntry);

						String textFileContent = IOUtils.toString(inputStream, "UTF-8");

						int indexOfDirectorySeparator = zipEntryFileName.indexOf('/');
						String cleanZipEntryFileName;

						if (indexOfDirectorySeparator != -1)
							cleanZipEntryFileName = zipEntryFileName.substring(
									indexOfDirectorySeparator + 1, zipEntryFileName.indexOf('.'));
						else
							cleanZipEntryFileName = zipEntryFileName;

						pageContentMap.put(cleanZipEntryFileName, textFileContent);
					}
				}
			}

		} catch (ZipException e) {
			logger.debug(e.getMessage());
		} catch (IOException e) {
			logger.debug(e.getMessage());
		}
	}

	/**
	 * Recurse on root directory and sub-directories
	 * 
	 * @param currentDirectory
	 */
	public void searchDirectory(File currentDirectory) {

		if (currentDirectory.isFile()) {

			String currentFileName = currentDirectory.getName();
			String volumeID = extractVolumeIDFromFilePath(currentFileName);
			//logger.debug(currentFileName);

			if (currentFileName.endsWith("xml")) {

				if (parseXML) {

					VolumeRecord currentVolumeRecord = new VolumeRecord(volumeID);

					parser.setMetsFile(currentDirectory);
					parser.setVolumeRecord(currentVolumeRecord);

					try {

						parser.parse();
						dbHelper.insertVolume(currentVolumeRecord);

					} catch (IOException e) {
						logger.debug(e.getMessage());
					} catch (XMLStreamException e) {
						logger.debug(e.getMessage());
					}
				}
			}

			else if (currentFileName.endsWith("zip")) {

				Map<String, String> pageContentMap = new HashMap<String, String>();

				getZipFileContent(currentDirectory, pageContentMap);

				long startTime = System.currentTimeMillis();
				dbHelper.insertVolume(volumeID, pageContentMap); // insert_volume
				long endTime = System.currentTimeMillis();
				this.timeTakenToInsertVolume += (endTime - startTime);
				this.totalVolumesInserted++;
			}

			return;
		}

		File[] filesInCurrentDir = currentDirectory.listFiles();

		for (File file : filesInCurrentDir) {
			searchDirectory(file);
		}
	}

	public void setDbHelper(DBHelper dbHelper) {
		this.dbHelper = dbHelper;
	}
}