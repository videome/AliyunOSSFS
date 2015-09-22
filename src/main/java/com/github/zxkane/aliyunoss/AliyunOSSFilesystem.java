package com.github.zxkane.aliyunoss;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSErrorCode;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;

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

	private OSSClient ossClient;

	private String bucketName;

	private int readMaxKeys = 1000;

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
		try {
			if ("/".equals(path)) {
				stat.setMode(NodeType.DIRECTORY, true, false, true, true, false, true, true, false, true);
			} else {
				ObjectMetadata objectMetadata = ossClient.getObjectMetadata(bucketName, path.substring(1));
				stat.setMode(NodeType.FILE, true, false, false, true, false, false, true, false, false);
				stat.setAllTimesMillis(objectMetadata.getLastModified().getTime());
			}
		} catch (OSSException e) {
			if (OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
				try {
					ObjectMetadata objectMetadata = ossClient.getObjectMetadata(bucketName, path.substring(1) + "/");
					stat.setMode(NodeType.DIRECTORY, true, false, false, true, false, false, true, false, false);
					stat.setAllTimesMillis(objectMetadata.getLastModified().getTime());
				} catch (OSSException e2) {
					if (OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
						return -ErrorCodes.ENOENT();
					}
					throw new IllegalStateException("Error reading path " + path, e);
				}
				return 0;
			}
			throw new IllegalStateException("Error reading path " + path, e);
		}
		return 0;
	}

	@Override
	public int read(final String path, final ByteBuffer buffer, final long size, final long offset, final FileInfoWrapper info) {
		try {
			GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, path.substring(1));
			getObjectRequest.setRange(offset, offset + size);

			OSSObject object = ossClient.getObject(getObjectRequest);

			InputStream objInput = object.getObjectContent();
			byte[] arr = new byte[(int) size];
			int read = objInput.read(arr, 0, (int) size);
			// -1 indicates EOF => nothing to put into the buffer
			if (read == -1) {
				return 0;
			}

			buffer.put(arr, 0, read);
			return read;
		} catch (OSSException e) {
			if (OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
				return -ErrorCodes.ENOENT();
			}
			throw new IllegalStateException("Error reading contents of path " + path, e);
		} catch (IOException e) {
			throw new IllegalStateException("Error reading contents of path " + path, e);
		}
	}

	@Override
	public int readdir(final String path, final DirectoryFiller filler) {
		if (path == null || !path.startsWith("/"))
			throw new IllegalStateException("Error reading directories in illegal path " + path);
		// 构造ListObjectsRequest请求
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest(bucketName);
		// "/" 为文件夹的分隔符
		listObjectsRequest.setDelimiter("/");
		listObjectsRequest.setMaxKeys(this.readMaxKeys);

		final String prefix = path.substring(1) + "/";
		if (!"/".equals(path)) {
			try {
				ossClient.getObjectMetadata(bucketName, prefix);
			} catch (OSSException e) {
				if (OSSErrorCode.NO_SUCH_KEY.equals(e.getErrorCode())) {
					throw new IllegalStateException("Error reading non-existing directory in path " + path);
				}
				throw new IllegalStateException("Error reading directory in path " + path, e);
			}
			// 列出目录下的所有文件和文件夹
			listObjectsRequest.setPrefix(prefix);
		}

		ObjectListing listing;
		do {
			listing = ossClient.listObjects(listObjectsRequest);
			// 遍历所有CommonPrefix
			for (String commonPrefix : listing.getCommonPrefixes()) {
				filler.add("/" + commonPrefix.substring(0, commonPrefix.length() - 1));
			}

			// 遍历所有Object
			for (OSSObjectSummary objectSummary : listing.getObjectSummaries()) {
				if (prefix.equals(objectSummary.getKey()))
					continue;
				filler.add(objectSummary.getKey());
			}

			listObjectsRequest.setMarker(listing.getNextMarker());
		} while (listing.isTruncated());

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
