package etu.splunk.hdfs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsUtil {

	private static FileSystem getFileSystem(URI uri, Configuration conf) {
		try {
			return FileSystem.get(uri, conf);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(2);
			return null;
		}
	}

	private static void verifyDirExistance(Path dir, FileSystem fs) {
		try {
			if (!(fs.exists(dir) && fs.isDirectory(dir))) {
				System.exit(2);
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(2);
		}
	}

	private static void printContentsOfPathToSystemOut(FileSystem fs, Path dir) {
		try {
			FileStatus[] files = fs.listStatus(dir);
			for (FileStatus file : files) {

				FSDataInputStream in = fs.open(file.getPath());

				while (true) {
					String line = in.readLine();
					if (line == null) {
						break;
					}

					System.out.println(line);
				}

				in.close();
				in = null;
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(2);
		}
	}

	public static void main(String args[]) {

		Configuration conf = new Configuration();
		URI uri = URI.create(args[0]);

		FileSystem fs = getFileSystem(uri, conf);
		Path dir = new Path(args[0]);

		verifyDirExistance(dir, fs);

		printContentsOfPathToSystemOut(fs, dir);

		System.exit(0);
	}

}
