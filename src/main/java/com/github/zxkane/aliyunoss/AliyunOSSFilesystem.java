package com.github.zxkane.aliyunoss;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;

import net.fusejna.DirectoryFiller;
import net.fusejna.FuseFilesystem;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.util.FuseFilesystemAdapterFull;

/**
 * Implementation of the {@link FuseFilesystem} interfaces to provide a view of
 * Aliyun's OSS bucket as a plain file system.
 *
 */
public class AliyunOSSFilesystem extends FuseFilesystemAdapterFull implements Closeable {
	private static final long CACHE_TIMEOUT = 60 * 1000; // one minute

	private OSSClient ossClient;

	private String bucketName;

	/**
	 * Don't print out a warning for some directories which are queried by some
	 * apps, e.g. Nautilus on Gnome
	 */
	private static Set<String> IGNORED_DIRS = new HashSet<String>();

	static {
		IGNORED_DIRS.add("/.hidden");
		IGNORED_DIRS.add("/.Trash");
		IGNORED_DIRS.add("/.Trash-1000");
	}

	public AliyunOSSFilesystem(OSSClient ossClient, String bucketName, boolean enableLogging) throws IOException {
		super();

		// disable verbose logging
		log(enableLogging);

		this.ossClient = ossClient;
		this.bucketName = bucketName;
	}

	@Override
	public int getattr(final String path, final StatWrapper stat) {
		return 0;
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info) {
		return 0;
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler) {
		if (path == null || !path.startsWith("/"))
			throw new IllegalStateException("Error reading directories in illegal path " + path);
		// 构造ListObjectsRequest请求
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);
		// "/" 为文件夹的分隔符
		listObjectsRequest.setDelimiter("/");

		if (!"/".equals(path)) {
			try {
				ossClient.getObjectMetadata(bucketName, path.substring(1) + "/");
			} catch (OSSException e) {
				if (OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
					throw new IllegalStateException("Error reading non-existing directory in path " + path);
				}
				throw new IllegalStateException("Error reading directory in path " + path + " due to " + e.getMessage());
			}
			// 列出目录下的所有文件和文件夹
			listObjectsRequest.setPrefix(path.substring(1) + "/");
		}
		ObjectListing listing = ossClient.listObjects(listObjectsRequest);

		// 遍历所有CommonPrefix
		for (String commonPrefix : listing.getCommonPrefixes()) {
			filler.add("/" + commonPrefix.substring(0, commonPrefix.length() - 1));
		}

		// 遍历所有Object
		for (OSSObjectSummary objectSummary : listing.getObjectSummaries()) {
			filler.add(objectSummary.getKey());
		}

		return 0;
	}

	@Override
	public int readlink(String path, ByteBuffer buffer, long size) {
		return 0;
	}

	/**
	 * Free up resources held for the OSS client and unmount the
	 * FUSE-filesystem.
	 *
	 * @throws IOException
	 *             If an error ocurred while closing the OSS client or while
	 *             unmounting the filesystem.
	 */
	@Override
	public void close() throws IOException {
		ossClient = null;
	}
}
