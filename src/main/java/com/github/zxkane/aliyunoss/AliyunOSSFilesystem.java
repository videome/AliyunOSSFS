package com.github.zxkane.aliyunoss;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.common.utils.IOUtils;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import net.fusejna.DirectoryFiller;
import net.fusejna.ErrorCodes;
import net.fusejna.FuseFilesystem;
import net.fusejna.StructFuseFileInfo.FileInfoWrapper;
import net.fusejna.StructStat.StatWrapper;
import net.fusejna.types.TypeMode.NodeType;
import net.fusejna.util.FuseFilesystemAdapterFull;

/**
 * Implementation of the {@link FuseFilesystem} interfaces to provide a view of
 * Aliyun's OSS bucket as a plain file system.
 *
 */
public class AliyunOSSFilesystem extends FuseFilesystemAdapterFull implements Closeable {

	private static final Logger logger = LoggerFactory.getLogger(AliyunOSSFilesystem.class);

	private OSSClient ossClient;

	private String bucketName;

	private int readMaxKeys = 1000;

	private static Set<String> IGNORED_DIRS = new HashSet<String>();

	private static final Object NULL = new Object();
	Cache<String, Object> notFoundObject = CacheBuilder.newBuilder().concurrencyLevel(1).maximumSize(1000).expireAfterWrite(3, TimeUnit.HOURS).build();

	// OSS using path with '/' to treat it as folder, actually it might don't
	// have the empty object representing a folder
	private Set<String> knownDirs = new HashSet<String>(1000);

	private static final String IGNORE_PREFIX;

	/**
	 * Those dirs or files might be retrieved in callback 'getattr' by shells or
	 * file explorers, ignore them to avoid unnecessary requests to Aliyun OSS.
	 */
	static {
		IGNORE_PREFIX = "._";

		IGNORED_DIRS.add("/._.");
		IGNORED_DIRS.add("/.git");
		IGNORED_DIRS.add("/HEAD");
		IGNORED_DIRS.add("/.DS_Store");
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

	public void setReadMaxKeys(int keys) {
		this.readMaxKeys = keys;
	}

	@Override
	public int getattr(final String path, final StatWrapper stat) {
		logger.debug("Getting attribute of path '{}'", path);
		if ("/".equals(path)) {
			stat.setMode(NodeType.DIRECTORY, true, false, true, true, false, true, true, false, true);
		} else if (IGNORED_DIRS.contains(path)) {
			logger.debug("Return not found the path '{}' in ignore list.", path);
			return -ErrorCodes.ENOENT();
		} else if (path.substring(path.lastIndexOf('/')).startsWith(IGNORE_PREFIX)) {
			logger.debug("Return not found the path '{}' with ingore prefix '{}'.", path, IGNORE_PREFIX);
			return -ErrorCodes.ENOENT();
		} else if (NULL.equals(notFoundObject.getIfPresent(path))) {
			logger.debug("Return not found the path '{}' in known not-found list.", path);
			return -ErrorCodes.ENOENT();
		} else {
			try {
				ObjectMetadata objectMetadata = ossClient.getObjectMetadata(bucketName, path.substring(1));
				stat.setMode(NodeType.FILE, true, false, false, true, false, false, true, false, false);
				stat.setAllTimesMillis(objectMetadata.getLastModified().getTime());
				stat.size(objectMetadata.getContentLength());
			} catch (OSSException e) {
				if (OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
					final String folderKey = path.substring(1) + "/";
					try {
						ObjectMetadata objectMetadata = ossClient.getObjectMetadata(bucketName, folderKey);
						stat.setMode(NodeType.DIRECTORY, true, false, true, true, false, true, true, false, true);
						stat.setAllTimesMillis(objectMetadata.getLastModified().getTime());
						stat.size(objectMetadata.getContentLength());
					} catch (OSSException e2) {
						if (OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
							if (knownDirs.contains(path.substring(1))) {
								stat.setMode(NodeType.DIRECTORY, true, false, true, true, false, true, true, false, true);
							} else {
								notFoundObject.put(path, NULL);
								logger.error("Can not find path '{}'.", path);
								return -ErrorCodes.ENOENT();
							}
						} else {
							logger.error("Error on reading attr of path '{}'.", path);
							throw new IllegalStateException("Error reading path " + path, e);
						}
					}
					logger.debug("Got attribute {} for path '{}'.", stat, path);
					return 0;
				}
				logger.error("Error on reading attr of path '{}'.", path);
				throw new IllegalStateException("Error reading path " + path, e);
			}
		}
		logger.debug("Got attribute {} for path '{}'.", stat, path);
		return 0;
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info) {
		logger.debug("Reading path '{}' with size {} from offset {}.", path, size, offset);
		try {
			GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, path.substring(1));
			getObjectRequest.setRange(offset, offset + size - 1);

			OSSObject object = ossClient.getObject(getObjectRequest);

			final InputStream objInput = object.getObjectContent();
			final int length = (int) object.getObjectMetadata().getContentLength();
			int read = 0;
			final int bufSize = 1024;
			try {
				if (length > size) {
					// anyway OSS API does not honor the range specified in
					// GetObjectRequest
					objInput.skip(offset);
				}
				byte[] readBuffer = new byte[bufSize];
				int bytesRead;
				while ((bytesRead = objInput.read(readBuffer)) > -1) {
					read += bytesRead;
					buffer.put(readBuffer, 0, bytesRead);
				}
			} finally {
				IOUtils.safeClose(objInput);
			}
			logger.debug("Read path '{}' with length {} from offset {}.", path, read, offset);
			return read;
		} catch (OSSException e) {
			if (OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
				logger.error("Can not find path '{}'.", path);
				return -ErrorCodes.ENOENT();
			}
			logger.error("Error on reading path '{}'.", path);
			throw new IllegalStateException("Error reading contents of path " + path, e);
		} catch (IOException e) {
			logger.error("Error on reading path '{}'.", path);
			throw new IllegalStateException("Error reading contents of path " + path, e);
		}
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler) {
		logger.debug("Read dir from path '{}'.", path);
		if (path == null || !path.startsWith("/")) {
			logger.error("Read dir from illegal path '{}'.", path);
			throw new IllegalStateException("Error reading directories in illegal path " + path);
		}
		// 构造ListObjectsRequest请求
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);
		// "/" 为文件夹的分隔符
		listObjectsRequest.setDelimiter("/");
		listObjectsRequest.setMaxKeys(this.readMaxKeys);

		final String folderName = path.substring(1);
		final String prefix = folderName + "/";
		if (!"/".equals(path)) {
			if (!knownDirs.contains(folderName)) {
				try {
					ossClient.getObjectMetadata(bucketName, prefix);
				} catch (OSSException e) {
					if (OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
						logger.error("Read dir from nonexisting path '{}'.", path);
						throw new IllegalStateException("Error reading non-existing directory in path " + path);
					}
					logger.error("Error on reading dir from path '{}'.", path);
					throw new IllegalStateException("Error reading directory in path " + path, e);
				}
			}
			// 列出目录下的所有文件和文件夹
			listObjectsRequest.setPrefix(prefix);
		}

		ObjectListing listing;
		do {
			listing = ossClient.listObjects(listObjectsRequest);
			// 遍历所有CommonPrefix
			for (String commonPrefix : listing.getCommonPrefixes()) {
				final String folderNameWithoutTrailer = commonPrefix.substring(0, commonPrefix.length() - 1);
				final String folderPath = "/" + folderNameWithoutTrailer;
				filler.add(folderPath);
				knownDirs.add(folderNameWithoutTrailer);
				if (notFoundObject.getIfPresent(folderPath) != null)
					notFoundObject.invalidate(folderPath);
			}

			// 遍历所有Object
			for (OSSObjectSummary objectSummary : listing.getObjectSummaries()) {
				if (prefix.equals(objectSummary.getKey()))
					continue;
				filler.add(objectSummary.getKey());
			}

			listObjectsRequest.setMarker(listing.getNextMarker());
		} while (listing.isTruncated());

		logger.debug("Read dir from path '{}' with result {}.", path, filler);
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
