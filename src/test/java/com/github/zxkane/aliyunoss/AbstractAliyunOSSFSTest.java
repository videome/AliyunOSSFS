package com.github.zxkane.aliyunoss;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ObjectMetadata;
import com.aliyun.oss.model.PutObjectResult;

public abstract class AbstractAliyunOSSFSTest {

	static final int LARGE_FILE_SIZE = 4888;

	static final int SMALL_FILE_SIZE = 400;

	static final String accessKeyId = System.getProperty("oss-key");
	static final String accessKeySecret = System.getProperty("oss-secret");
	static final String endpoint = System.getProperty("oss-endpoint", "oss-cn-beijing.aliyuncs.com");
	static final String bucketName = System.getProperty("oss-bucket", RandomStringUtils.random(8, 'a', 'z', true, true));

	static final String EXISTING_FOLDER_NAME = RandomStringUtils.random(8, true, true);

	static final String EXISTING_FILE1_PATH = RandomStringUtils.random(8, true, true);
	static final String EXISTING_FILE2_PATH = RandomStringUtils.random(8, true, true);
	static final String EXISTING_FILE_IN_FOLDER_PATH = EXISTING_FOLDER_NAME + "/" + RandomStringUtils.random(8, true, true);
	static final String EXISTING_LARGEFILE_IN_FOLDER_PATH = EXISTING_FOLDER_NAME + "/" + RandomStringUtils.random(8, true, true);

	static OSSClient client;

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
}
