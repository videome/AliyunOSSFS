package com.github.zxkane.aliyunoss;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class AliyunOSSFSTest extends AbstractAliyunOSSFSTest {

	@Before
	public void setup() {
		AliyunOSSFS.accessId = accessKeyId;
		AliyunOSSFS.accessKey = accessKeySecret;
		AliyunOSSFS.endpoint = endpoint;
	}

	@Test
	public void testConstruct() throws Exception {
		assertNotNull(new AliyunOSSFS());
	}

	@Test
	public void testMainMultiple() throws Exception {
		File mountPoint = File.createTempFile("AliOSSTest", ".dir");
		try {
			// if we have one that works and the last one an invalid one we get
			// an exception, but did the mounting
			// for the first one
			AliyunOSSFS
					.main(new String[] { "-i", accessKeyId, "-k", accessKeySecret, "-e", endpoint, "-b", "nonsuchbucket", "-m", mountPoint.getAbsolutePath() });
			fail("Should throw exception with invalid oss bucket");
		} catch (IOException e) {
			// happens when run in CloudBees, but could not find out details...
		} catch (IllegalStateException e) {
			assertTrue("Had: " + e.getMessage(), e.getMessage().contains("invalidrepo"));
		}
	}

	@Test
	public void testMount() throws Exception {
		try {
			assertFalse(AliyunOSSFS.unmount("notexisting"));

			// if we have one that works and the last one an invalid one we get
			// an exception, but did the mounting
			// for the first one
			File mountPoint = File.createTempFile("AliOSSTest", ".dir");
			assertTrue(mountPoint.delete());
			try {
				AliyunOSSFS.mount(bucketName, mountPoint);
				AliyunOSSFS.list();
				assertTrue(AliyunOSSFS.unmount(bucketName));
			} finally {
				FileUtils.deleteDirectory(mountPoint);

			}
		} catch (IOException e) {
			// happens when run in CloudBees, but could not find out details...
			Assume.assumeNoException("In some CI environments this will fail", e);
		} catch (UnsatisfiedLinkError e) {
			Assume.assumeNoException("Will fail on Windows", e);
		}
	}

	@Test
	public void testMountBucketTwice() throws Exception {
		try {
			// if we have one that works and the last one an invalid one we get
			// an exception, but did the mounting
			// for the first one
			File mountPoint = File.createTempFile("AliOSSTest", ".dir");
			assertTrue(mountPoint.delete());
			try {
				AliyunOSSFS.mount(bucketName, mountPoint);
				try {
					AliyunOSSFS.list();

					try {
						AliyunOSSFS.mount(bucketName, mountPoint);
						fail("Should fail due to double mount here");
					} catch (IllegalArgumentException e) {
						assertTrue(e.getMessage().contains("already mounted"));
					}
				} finally {
					assertTrue(AliyunOSSFS.unmount(mountPoint.getPath()));
				}
			} finally {
				FileUtils.deleteDirectory(mountPoint);
			}
		} catch (IOException e) {
			// happens when run in CloudBees, but could not find out details...
			Assume.assumeNoException("In some CI environments this will fail", e);
		} catch (UnsatisfiedLinkError e) {
			Assume.assumeNoException("Will fail on Windows", e);
		}
	}

	@Test
	public void testMountPointTwice() throws Exception {
		final String bucketName2 = System.getProperty(RandomStringUtils.random(8, 'a', 'z', true, true));
		try {
			// if we have one that works and the last one an invalid one we get
			// an exception, but did the mounting
			// for the first one
			File mountPoint = File.createTempFile("AliOSSTest", ".dir");
			assertTrue(mountPoint.delete());
			try {
				AliyunOSSFS.mount(bucketName, mountPoint);
				try {
					AliyunOSSFS.list();

					try {
						AliyunOSSFS.mount(bucketName2, mountPoint);
						fail("Should fail due to double mount here");
					} catch (IllegalArgumentException e) {
						assertTrue(e.getMessage().contains("already used for mount at"));
					}
				} finally {
					assertTrue(AliyunOSSFS.unmount(mountPoint.getPath()));
				}
			} finally {
				FileUtils.deleteDirectory(mountPoint);
			}
		} catch (IOException e) {
			// happens when run in CloudBees, but could not find out details...
			Assume.assumeNoException("In some CI environments this will fail", e);
		} catch (UnsatisfiedLinkError e) {
			Assume.assumeNoException("Will fail on Windows", e);
		}
	}
}
