package com.github.zxkane.aliyunoss;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectResult;

import net.fusejna.DirectoryFiller;

public class AliyunOSSFilesystemTest {

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

	@BeforeClass
	public static void setUpBucket() throws IOException {
		client = new OSSClient(endpoint, accessKeyId, accessKeySecret);
		client.createBucket(bucketName);

		createFolder(EXISTING_FOLDER_NAME);
		Arrays.asList(new String[] { EXISTING_FILE1_PATH, EXISTING_FILE2_PATH, EXISTING_FILE_IN_FOLDER_PATH }).forEach((key) -> {
			createFile(key);
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

	static void createFile(String key) {
		final String content = "abcdefg";
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
		Arrays.asList(new String[] { EXISTING_FILE1_PATH, EXISTING_FILE2_PATH, EXISTING_FILE_IN_FOLDER_PATH, EXISTING_FOLDER_NAME + "/" }).forEach((key) -> {
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
}