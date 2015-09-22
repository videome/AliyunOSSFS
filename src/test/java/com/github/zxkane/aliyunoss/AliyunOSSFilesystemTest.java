package com.github.zxkane.aliyunoss;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectResult;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseException;
import net.fusejna.StatWrapperFactory;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;

public class AliyunOSSFilesystemTest {

	static final int LARGE_FILE_SIZE = 4888;

	static final int SMALL_FILE_SIZE = 400;

	private static OSSClient client;

	private AliyunOSSFilesystem fs;

	static final String accessKeyId = System.getProperty("oss-key");
	static final String accessKeySecret = System.getProperty("oss-secret");
	static final String endpoint = System.getProperty("oss-endpoint", "oss-cn-beijing.aliyuncs.com");
	static final String bucketName = System.getProperty("oss-bucket", RandomStringUtils.random(8, 'a', 'z', true, true));

	static final String EXISTING_FOLDER_NAME = RandomStringUtils.random(8, true, true);

	static final String EXISTING_FILE1_PATH = RandomStringUtils.random(8, true, true);
	static final String EXISTING_FILE2_PATH = RandomStringUtils.random(8, true, true);
	static final String EXISTING_FILE_IN_FOLDER_PATH = EXISTING_FOLDER_NAME + "/" + RandomStringUtils.random(8, true, true);
	static final String EXISTING_LARGEFILE_IN_FOLDER_PATH = EXISTING_FOLDER_NAME + "/" + RandomStringUtils.random(8, true, true);

	@BeforeClass
	public static void setUpBucket() throws IOException {
		client = new OSSClient(endpoint, accessKeyId, accessKeySecret);
		client.createBucket(bucketName);

		createFolder(EXISTING_FOLDER_NAME);
		Arrays.asList(new String[] { EXISTING_FILE1_PATH, EXISTING_FILE2_PATH, EXISTING_FILE_IN_FOLDER_PATH }).forEach((key) -> {
			createSmallFile(key);
		});
		Arrays.asList(new String[] { EXISTING_LARGEFILE_IN_FOLDER_PATH }).forEach((key) -> {
			createLargeFile(key);
		});
	}

	static void createFolder(String folderName) throws IOException {
		final String objectName = folderName + "/";
		ObjectMetadata objectMeta = new ObjectMetadata();
		/*
		 * 这里的size为0,注意OSS本身没有文件夹的概念,这里创建的文件夹本质上是一个size为0的Object,
		 * dataStream仍然可以有数据
		 */
		byte[] buffer = new byte[0];
		ByteArrayInputStream in = new ByteArrayInputStream(buffer);
		objectMeta.setContentLength(0);
		try {
			client.putObject(bucketName, objectName, in, objectMeta);
		} finally {
			in.close();
		}
	}

	static void createSmallFile(String key) {
		final String content = RandomStringUtils.random(SMALL_FILE_SIZE, true, true);
		createFile(key, content);
	}

	static void createLargeFile(String key) {
		final String content = RandomStringUtils.random(LARGE_FILE_SIZE, true, true);
		createFile(key, content);
	}

	static void createFile(final String key, final String content) {
		InputStream input = new ByteArrayInputStream(content.getBytes());
		// 创建上传Object的Metadata
		ObjectMetadata meta = new ObjectMetadata();
		// 必须设置ContentLength
		meta.setContentLength(content.getBytes().length);
		// 上传Object.
		PutObjectResult result = client.putObject(bucketName, key, input, meta);
	}

	@AfterClass
	public static void cleanBucket() {
		Arrays.asList(new String[] { EXISTING_FILE1_PATH, EXISTING_FILE2_PATH, EXISTING_FILE_IN_FOLDER_PATH, EXISTING_LARGEFILE_IN_FOLDER_PATH,
				EXISTING_FOLDER_NAME + "/" }).forEach((key) -> {
					client.deleteObject(bucketName, key);
				});
		client.deleteBucket(bucketName);
	}

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
