/*
#
# Copyright 2013 The Trustees of Indiana University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -----------------------------------------------------------------
#
# Project: mets-parser
# File:  METSParserTest.java
# Description:  
#
# -----------------------------------------------------------------
# 
 */

/**
 * 
 */
package test.java.b699.mets;

import gov.loc.repository.pairtree.Pairtree;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import main.java.b669.mets.METSParser;
import main.java.b669.mets.METSParser.CopyrightEnum;
import main.java.b669.mets.METSParser.VolumeRecord;
import main.java.b669.mets.METSParser.VolumeRecord.PageRecord;

/**
 * @author Yiming Sun
 * 
 */
public class METSParserTest {

	// You are expected to extract the real volume ID from the Pairtree path.
	// A real volume ID is of the form:
	// <prefix>.<local_id>
	//
	// When a real volume ID is mapped to a Pairtree path, its <prefix> is
	// removed and placed above the Pairtree as a directory,
	// and the prefix directory is immediately followed by a directory named
	// "pairtree_root" to signify the beginning of a Pairtree path.
	//
	// The <local_id> portion of the volume ID undergoes the Pairtree clean
	// process to escape and/or replace all characters that
	// are not filesystem safe, and the "cleaned local ID" is then broken down
	// into a series of "shorties" and "morties" to define the
	// directory hierarchy in the Pairtree path. The cleaned local ID in its
	// entirety is also used to name the directory that terminates the
	// Pairtree path. The resources, i.e. the zip file and the METS xml file are
	// placed under the terminating directory, and they
	// both use the cleaned local ID as their filenames for easy identification.
	//
	// As in the example used by this code, the volume ID is
	//
	// loc.ark:/13960/t9765kx0j
	//
	// its <prefix> is "loc", and its <local_id> is "ark:/13960/t9765kx0j",
	// notice the filesystem unsafe characters such as ":" and "/".
	// After the Pairtree cleaning process, the <local_id> becomes
	// "ark+=13960=t9765kx0j"
	//
	// The corresponding Pairtree path with prefix for this volume ID is:
	//
	// loc/pairtree_root/ar/k+/=1/39/60/=t/97/65/kx/0j/ark+=13960=t9765kx0j/
	//
	// and the resources, the zip file and the METS xml file have the following
	// paths, respectively:
	//
	// loc/pairtree_root/ar/k+/=1/39/60/=t/97/65/kx/0j/ark+=13960=t9765kx0j/ark+=13960=t9765kx0j.zip
	// loc/pairtree_root/ar/k+/=1/39/60/=t/97/65/kx/0j/ark+=13960=t9765kx0j/ark+=13960=t9765kx0j.mets.xml

	static final String zipPath = "loc/pairtree_root/ar/k+/=1/39/60/=t/97/65/kx/0j/ark+=13960=t9765kx0j/ark+=13960=t9765kx0j.zip";
	static final String metsPath = "loc/pairtree_root/ar/k+/=1/39/60/=t/97/65/kx/0j/ark+=13960=t9765kx0j/ark+=13960=t9765kx0j.mets.xml";

	static final String additionalDir = "target/resources";
	static VolumeRecord volumeRecord = null;

	/**
	 * Setup test harness
	 * 
	 * @throws Exception
	 */
	@BeforeClass
	public static void setup() throws Exception {

		parseMETS();
	}

	/**
	 * This method shows how to use METSParser to parse a METS file
	 * 
	 * @throws IOException
	 * @throws XMLStreamException
	 */
	private static void parseMETS() throws IOException, XMLStreamException {

		File metsFile = new File(additionalDir, metsPath);

		if (!metsFile.exists()) {
			throw new IOException("File does not exist. Current path is "
					+ metsFile.getAbsolutePath());
		}
		String volumeID = extractVolumeIDFromFilePath(metsPath);
		XMLInputFactory xmlInputFactory = XMLInputFactory.newInstance();

		volumeRecord = new VolumeRecord(volumeID);

		// copyright is assumed to be public domain for all volumes
		volumeRecord.setCopyright(CopyrightEnum.PUBLIC_DOMAIN);

		METSParser metsParser = new METSParser(metsFile, volumeRecord,
				xmlInputFactory);
		metsParser.parse();

		// that's it. it is now parsed and the volumeRecord should be populated
		// by the parser
		// use volumeRecord to retrieve the information (see test cases on what
		// information is there)

	}

	private static String extractVolumeIDFromFilePath(String path) {

		// Here I am taking a shortcut by using known prefix and Pairtree
		// cleaned token directly for brevity.
		// But you are supposed to extract the information from the actual path

		// prefix should be "loc"
		String prefix = "loc";

		String filename = "ark+=13960=t9765kx0j.mets.xml";

		// the full filename sans the extension is the cleaned headless ID
		String cleanedHeadlessID = filename.substring(0, filename.length()
				- ".mets.xml".length());

		// must unclean this token using Pairtree
		Pairtree pairtree = new Pairtree();
		String uncleanedHeadlessID = pairtree.uncleanId(cleanedHeadlessID);

		String str = prefix + "." + uncleanedHeadlessID;

		return prefix + "." + uncleanedHeadlessID;

	}

	@Test
	public void testPageCount() {
		int expectedPageCount = 50;
		int actualPageCount = volumeRecord.getPageCount();

		Assert.assertEquals(expectedPageCount, actualPageCount);
	}

	@Test
	public void testPageFilenameSet() {
		Set<String> expectedPageFilenameSet = new HashSet<String>();
		for (int i = 1; i <= 50; i++) {
			String filename = String.format("%08d.txt", i);
			expectedPageFilenameSet.add(filename);
		}

		Set<String> actualPageFilenameSet = volumeRecord.getPageFilenameSet();

		Assert.assertEquals(true,
				actualPageFilenameSet.containsAll(expectedPageFilenameSet));
		Assert.assertEquals(true,
				expectedPageFilenameSet.containsAll(actualPageFilenameSet));
	}

	@Test
	public void testPageFeature1() {
		final String filename1 = "00000001.txt";
		String[] expectedPageFeaturesArray = { "RIGHT", "COVER" };
		List<String> expectedPageFeatures = Arrays
				.asList(expectedPageFeaturesArray);

		PageRecord pageRecord = volumeRecord.getPageRecordByFilename(filename1);
		List<String> actualPageFeatures = pageRecord.getFeatures();

		Assert.assertEquals(true,
				actualPageFeatures.containsAll(expectedPageFeatures));
		Assert.assertEquals(true,
				expectedPageFeatures.containsAll(actualPageFeatures));
	}

	@Test
	public void testPageByteCount() {
		final String filename1 = "00000006.txt";
		long expectedByteCount = 1505;
		PageRecord pageRecord = volumeRecord.getPageRecordByFilename(filename1);
		long actualByteCount = pageRecord.getByteCount();

		Assert.assertEquals(expectedByteCount, actualByteCount);
	}

	@Test
	public void testPageChecksum() {
		final String filename1 = "00000043.txt";
		final String expectedChecksum = "3a44bbfbcb262c592b79b307716b0a41";
		final String expectedChecksumType = "MD5";
		PageRecord pageRecord = volumeRecord.getPageRecordByFilename(filename1);
		String actualChecksum = pageRecord.getChecksum();
		String actualChecksumType = pageRecord.getChecksumType();

		Assert.assertEquals(expectedChecksum, actualChecksum);
		Assert.assertEquals(expectedChecksumType, actualChecksumType);
	}

	@Test
	public void testPageLabel() {
		final String filename1 = "00000008.txt";
		final String expectedLabel = "4";
		PageRecord pageRecord = volumeRecord.getPageRecordByFilename(filename1);
		String actualLabel = pageRecord.getLabel();

		Assert.assertEquals(expectedLabel, actualLabel);
	}

	@Test
	public void testPageOrder() {
		final String filename1 = "00000013.txt";
		final int expectedOrder = 13;
		PageRecord pageRecord = volumeRecord.getPageRecordByFilename(filename1);
		int actualOrder = pageRecord.getOrder();

		Assert.assertEquals(expectedOrder, actualOrder);

	}

	@Test
	public void testPageSequence() {
		final String filename1 = "00000027.txt";
		final String expectedSequence = "00000027";
		PageRecord pageRecord = volumeRecord.getPageRecordByFilename(filename1);
		String actualSequence = pageRecord.getSequence();

		Assert.assertEquals(expectedSequence, actualSequence);
	}

	@Test
	public void testVolumeID() {
		final String expectedVolumeID = "loc.ark:/13960/t9765kx0j";
		String actualVolumeID = volumeRecord.getVolumeID();

		Assert.assertEquals(expectedVolumeID, actualVolumeID);
	}

	@Test
	public void testByteCountFromZip() throws Exception {
		ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(
				new File(additionalDir, zipPath)));
		ZipEntry zipEntry = null;
		final int BUFFER_SIZE = 65535;
		byte[] buffer = new byte[BUFFER_SIZE];
		do {
			zipEntry = zipInputStream.getNextEntry();
			if (zipEntry != null) {
				String entryName = zipEntry.getName();
				if (entryName.matches("\\d{8}.txt")) {
					ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
					int read = 0;
					do {
						read = zipInputStream.read(buffer);
						if (read > 0) {
							byteArrayOutputStream.write(buffer, 0, read);
						}
					} while (read > 0);
					zipInputStream.closeEntry();
					long actualByteCount = byteArrayOutputStream.size();
					PageRecord pageRecord = volumeRecord
							.getPageRecordByFilename(entryName);
					long expectedByteCount = pageRecord.getByteCount();
					Assert.assertEquals(expectedByteCount, actualByteCount);
				}
				zipInputStream.closeEntry();
			}
		} while (zipEntry != null);
		zipInputStream.close();
	}

}
