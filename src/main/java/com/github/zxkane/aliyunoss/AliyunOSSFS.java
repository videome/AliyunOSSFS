package com.github.zxkane.aliyunoss;

import static java.util.Arrays.asList;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.OSSClient;
import com.github.zxkane.aliyunoss.console.Console;
import com.github.zxkane.aliyunoss.util.FuseUtils;
import com.google.common.base.Preconditions;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import net.fusejna.FuseException;

public class AliyunOSSFS {

	private static final Logger logger = LoggerFactory.getLogger(AliyunOSSFS.class);

	private static final ConcurrentMap<Pair<String, String>, Pair<File, AliyunOSSFilesystem>> mounts = new ConcurrentHashMap<>();
	static String accessId, accessKey, endpoint;

	public static void main(String[] args) throws UnsatisfiedLinkError, IllegalArgumentException, IOException, FuseException {
		File defaultCredentialFile = new File(new File(System.getProperty("user.home")), ".aliyuncli/osscredentials");
		OptionParser parser = new OptionParser() {
			{
				accepts("i").withRequiredArg().ofType(String.class).describedAs("accessId");
				accepts("k").requiredIf("i").withRequiredArg().ofType(String.class).describedAs("accessKey");
				accepts("f").withRequiredArg().ofType(File.class).defaultsTo(defaultCredentialFile).describedAs("credential file");
				accepts("e").requiredIf("i").withRequiredArg().ofType(String.class).describedAs("endpoint");
				accepts("b").withRequiredArg().ofType(String.class).describedAs("bucketName");
				accepts("m").withRequiredArg().ofType(File.class).describedAs("mountpoint");
				acceptsAll(asList("h", "?"), "show help").forHelp();
			}
		};

		OptionSet options = parser.parse(args);

		if ((options.has("i") && !(options.has("k") && options.has("e"))) || !options.has("b") || !options.has("m")) {
			parser.printHelpOn(System.out);
			System.exit(1);
		} else {
			if (!options.has("i")) {
				final File credentialFile = new File(options.valueOf("f").toString());
				Properties prop = new Properties();
				try {
					InputStream input = new FileInputStream(credentialFile);
					try {
						prop.load(input);
						accessId = prop.getProperty("accessid");
						accessKey = prop.getProperty("accesskey");
						endpoint = prop.getProperty("host");
					} finally {
						input.close();
					}
				} catch (FileNotFoundException e) {
					System.err.println("Credential file does not exist.");
					System.exit(2);
				}
			} else {
				accessId = options.valueOf("i").toString();
				accessKey = options.valueOf("k").toString();
				endpoint = options.valueOf("e").toString();
			}

			try {
				mount(options.valueOf("b").toString(), new File(options.valueOf("m").toString()));

				InputStream inStream = new FileInputStream(FileDescriptor.in);
				new Console().run(inStream, System.out);
			} finally {
				// ensure that we try to close all filesystems that we created
				for (Pair<File, AliyunOSSFilesystem> ossFS : mounts.values()) {
					ossFS.getRight().close();
				}
			}
		}
	}

	/**
	 * Create a mount of the given bucket of Aliyun OSS at the given mount
	 * point. Will throw an exception if any of the mount operations fail or
	 * either the access issue of Aliyun OSS or the mount point is already used
	 * for another mount.
	 *
	 * @param accessId
	 *            access id of aliyunn oss
	 * @param accessKey
	 *            access key of aliyun oss
	 * @param endpoint
	 *            endpoint of aliyun oss
	 * @param bucketName
	 *            bucket name of aliyun oss
	 * @param mountPoint
	 *            The point in the filesystem where the Git Repository should
	 *            appear.
	 * @throws IOException
	 *             If a file operation fails during creating the mount.
	 * @throws UnsatisfiedLinkError
	 *             If an internal error occurs while setting up the mount.
	 * @throws FuseException
	 *             If an internal error occurs while setting up the mount.
	 * @throws IllegalArgumentException
	 *             If the git repository is already mounted somewhere or the
	 *             mount point is already used for another mount operation.
	 */
	public static void mount(String bucketName, File mountPoint) throws IOException, UnsatisfiedLinkError, FuseException, IllegalArgumentException {
		logger.info("Mounting Aliyun OSS bucket {} owned by {} from {} at mountpoint {}.", bucketName, accessId, endpoint, mountPoint);

		// don't allow double-mounting of the git-directory although it should
		// theoretically work on different mountpoints
		final Pair<String, String> mountIdentify = Pair.of(accessId, bucketName);
		Preconditions.checkArgument(!mounts.containsKey(mountIdentify), "Cannot mount OSS identify '%s' which is already mounted to %s.", mountIdentify,
				mounts.get(mountIdentify) == null ? null : mounts.get(mountIdentify).getLeft());

		// don't allow double-mounting on the same mount-point, this will fail
		// anyway
		for (Map.Entry<Pair<String, String>, Pair<File, AliyunOSSFilesystem>> entry : mounts.entrySet()) {
			Preconditions.checkArgument(!entry.getValue().getKey().equals(mountPoint), "Cannot mount to mount point '%s' which is already used for OSS %s.",
					mountPoint, entry.getKey());
		}

		// now create the Aliyun OSS filesystem
		AliyunOSSFilesystem ossFS = new AliyunOSSFilesystem(new OSSClient(endpoint, accessId, accessKey), bucketName, false);

		// ensure that we do not have a previous mount lingering on the
		// mountpoint
		FuseUtils.prepareMountpoint(mountPoint);

		// mount the filesystem. If this is the last mount-point that was
		// specified and no console is used
		// then block until the filesystem is unmounted
		ossFS.mount(mountPoint, false);

		mounts.put(mountIdentify, Pair.of(mountPoint, ossFS));
	}

	/**
	 * Prints out a list of currently mounted Aliyun OSS buckets.
	 */
	public static void list() {
		for (Map.Entry<Pair<String, String>, Pair<File, AliyunOSSFilesystem>> entry : mounts.entrySet()) {
			System.out.println("Bucket " + entry.getValue() + " mounted at " + entry.getValue().getLeft());
		}
	}

	/**
	 * Unmount the given mounting and free related system resources.
	 *
	 * @param dirOrMountPoint
	 *            Either the location of the git repository or the mount point.
	 * @return true if the directory was unmounted successfully, false if it was
	 *         not found and an exception is thrown if an error occurs during
	 *         unmounting.
	 * @throws IOException
	 */
	public static boolean unmount(String dirOrMountPoint) throws IOException {
		for (Map.Entry<Pair<String, String>, Pair<File, AliyunOSSFilesystem>> entry : mounts.entrySet()) {
			Pair<String, String> bucketIdentity = entry.getKey();
			if (entry.getValue().getLeft().getPath().equals(dirOrMountPoint)) {
				System.out.println("Unmounting Aliyun OSS bucket at " + bucketIdentity + " at mountpoint " + entry.getValue().getLeft());
				entry.getValue().getRight().close();
				mounts.remove(bucketIdentity);
				return true;
			}
		}

		System.out.println("Could not find " + dirOrMountPoint);
		return false;
	}
}
