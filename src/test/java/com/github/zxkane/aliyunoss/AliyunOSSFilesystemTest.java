package com.github.zxkane.aliyunoss;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.StatWrapperFactory;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;

public class AliyunOSSFilesystemTest extends AbstractAliyunOSSFSTest {

	private AliyunOSSFilesystem fs;

	@Before
	public void setUp() throws IOException {
		fs = new AliyunOSSFilesystem(client, bucketName, true);
	}

	@After
	public void tearDown() throws IOException {
		fs.close();
	}

	@Test
	public void testGetAttr() throws IOException, FuseException {
		StatWrapper stat = getStatsWrapper();
		assertEquals(0, fs.getattr("/", stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr("/" + EXISTING_FOLDER_NAME, stat));
		assertEquals(NodeType.DIRECTORY, stat.type());
		assertEquals(0, fs.getattr("/" + EXISTING_FILE1_PATH, stat));
		assertEquals(NodeType.FILE, stat.type());
		assertEquals(0, fs.getattr("/" + EXISTING_FILE2_PATH, stat));
		assertEquals(NodeType.FILE, stat.type());
		assertEquals(0, fs.getattr("/" + EXISTING_FILE_IN_FOLDER_PATH, stat));
		assertEquals(NodeType.FILE, stat.type());
		String path = "/" + EXISTING_FOLDER_NAME + "/notexist.txt";
		assertEquals(-ErrorCodes.ENOENT(), fs.getattr(path, stat));
		// invalid top-level-dir causes ENOENT
		assertEquals(-ErrorCodes.ENOENT(), fs.getattr("/notexistingmain", stat));

	}

	private StatWrapper getStatsWrapper() {
		final StatWrapper wrapper;
		try {
			wrapper = StatWrapperFactory.create();
		} catch (UnsatisfiedLinkError e) {
			System.out.println("This might fail on machines without fuse-binaries.");
			e.printStackTrace();
			Assume.assumeNoException(e); // stop test silently
			return null;
		} catch (NoClassDefFoundError e) {
			System.out.println("This might fail on machines without fuse-binaries.");
			e.printStackTrace();
			Assume.assumeNoException(e); // stop test silently
			return null;
		}
		return wrapper;
	}

	@Test
	public void testReadDir() {
		final List<String> filledFiles = new ArrayList<String>();
		DirectoryFiller filler = new DirectoryFillerImplementation(filledFiles);

		fs.readdir("/", filler);
		final String[] expected = new String[] { "/" + EXISTING_FOLDER_NAME, EXISTING_FILE1_PATH, EXISTING_FILE2_PATH };
		Arrays.sort(expected);
		final String[] result = filledFiles.toArray(new String[filledFiles.size()]);
		Arrays.sort(result);
		assertArrayEquals(expected, result);
	}

	@Test
	public void testReadDirMoreThanMaxKeys() {
		fs.setReadMaxKeys(3);

		List<String> keys = new ArrayList<String>(10);

		for (int i = 0; i < 10; i++) {
			String key = EXISTING_FOLDER_NAME + "/" + RandomStringUtils.random(8, true, true);
			createSmallFile(key);
			keys.add(key);
		}
		try {
			final List<String> filledFiles = new ArrayList<String>();
			DirectoryFiller filler = new DirectoryFillerImplementation(filledFiles);

			assertEquals(0, fs.readdir("/" + EXISTING_FOLDER_NAME, filler));
			keys.add(EXISTING_FILE_IN_FOLDER_PATH);
			keys.add(EXISTING_LARGEFILE_IN_FOLDER_PATH);
			final String[] expected = keys.toArray(new String[keys.size()]);
			Arrays.sort(expected);
			final String[] result = filledFiles.toArray(new String[filledFiles.size()]);
			Arrays.sort(result);
			assertArrayEquals(expected, result);
		} finally {
			keys.forEach((key) -> {
				client.deleteObject(bucketName, key);
			});
		}
	}

	@Test
	public void testReadDirPathFails() {
		final List<String> filledFiles = new ArrayList<String>();
		DirectoryFiller filler = new DirectoryFillerImplementation(filledFiles);

		String path = "/notexisting";
		try {
			filledFiles.clear();
			fs.readdir(path, filler);
			fail("Should throw exception as this should not occur");
		} catch (IllegalStateException e) {
			assertTrue(e.toString(), e.toString().contains("Error reading non-existing directory in path"));
			assertTrue(e.toString(), e.toString().contains(path));
		}
	}

	@Test
	public void testReadDirFails() {
		try {
			fs.readdir("/" + EXISTING_FILE1_PATH, null);
			fail("Should throw exception as this should not occur");
		} catch (IllegalStateException e) {
			assertTrue(e.toString(), e.toString().contains("Error reading non-existing directory in path"));
			assertTrue(e.toString(), e.toString().contains("/" + EXISTING_FILE1_PATH));
		}
	}

	private final class DirectoryFillerImplementation implements DirectoryFiller {
		private final List<String> filledFiles;

		private DirectoryFillerImplementation(List<String> filledFiles) {
			this.filledFiles = filledFiles;
		}

		@Override
		public boolean add(String... files) {
			for (String file : files) {
				filledFiles.add(file);
			}
			return true;
		}

		@Override
		public boolean add(Iterable<String> files) {
			for (String file : files) {
				filledFiles.add(file);
			}
			return true;
		}
	}

	@Test
	public void testRead() {
		assertEquals(100, fs.read("/" + EXISTING_FILE1_PATH, ByteBuffer.allocate(100), 100, 0, null));
	}

	@Test
	public void testReadTooMuch() {
		int read = fs.read("/" + EXISTING_FILE1_PATH, ByteBuffer.allocate(100000), 100000, 0, null);
		assertEquals(SMALL_FILE_SIZE, read);
	}

	@Test
	public void testReadWayTooMuch() {
		try {
			fs.read("/" + EXISTING_FILE1_PATH, ByteBuffer.allocate(100000), Integer.MAX_VALUE, 0, null);
			fail("Should throw exception as this should not occur");
		} catch (OutOfMemoryError e) {
			assertTrue(e.toString(), e.toString().contains("exceeds VM limit") || e.toString().contains("Java heap space"));
		}
	}

	@Test
	public void testReadNonexistingFails() {
		assertEquals(-ErrorCodes.ENOENT(), fs.read("/noexist", null, 0, 0, null));
	}
}
