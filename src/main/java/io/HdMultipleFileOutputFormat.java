package io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * TextOutputFormat extension which enables writing the mapper/reducer's output in multiple files.<br>
 * <p>
 * <b>WARNING</b>: The number of different folder shuoldn't be large for one mapper since we keep an
 * {@link RecordWriter} instance per folder name.
 * </p>
 * <p>
 * In this class the folder name is defined by the written entry's key.<br>
 * To change this behavior simply extend this class and override the
 * {@link HdMultipleFileOutputFormat#getFolderNameExtractor()} method and create your own
 * {@link FolderNameExtractor} implementation.
 * </p>
 * 
 * 
 * @author ykesten
 * 
 * @param <K> - Keys type
 * @param <V> - Values type
 */
public class HdMultipleFileOutputFormat<K, V> extends TextOutputFormat<K, V> {

	private String folderName;

	private class MultipleFilesRecordWriter extends RecordWriter<K, V> {

		private Map<String, RecordWriter<K, V>> fileNameToWriter;
		private FolderNameExtractor<K, V> fileNameExtractor;
		private TaskAttemptContext job;

		public MultipleFilesRecordWriter(FolderNameExtractor<K, V> fileNameExtractor, TaskAttemptContext job) {
			fileNameToWriter = new HashMap<String, RecordWriter<K, V>>();
			this.fileNameExtractor = fileNameExtractor;
			this.job = job;
		}

		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			String fileName = fileNameExtractor.extractFolderName(key, value);
			RecordWriter<K, V> writer = fileNameToWriter.get(fileName);
			if (writer == null) {
				writer = createNewWriter(fileName, fileNameToWriter, job);
				if (writer == null) {
					throw new IOException("Unable to create writer for path: " + fileName);
				}
			}
			writer.write(key, value);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			for (Entry<String, RecordWriter<K, V>> entry : fileNameToWriter.entrySet()) {
				entry.getValue().close(context);
			}
		}

	}

	private synchronized RecordWriter<K, V> createNewWriter(String folderName, Map<String, RecordWriter<K, V>> fileNameToWriter, TaskAttemptContext job) {
		try {
			this.folderName = folderName;
			RecordWriter<K, V> writer = super.getRecordWriter(job);
			this.folderName = null;
			fileNameToWriter.put(folderName, writer);
			return writer;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
		Path path = super.getDefaultWorkFile(context, extension);
		if (folderName != null) {
			String newPath = path.getParent().toString() + "/" + folderName + "/" + path.getName();
			path = new Path(newPath);
		}
		return path;
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		return new MultipleFilesRecordWriter(getFolderNameExtractor(), job);
	}

	public FolderNameExtractor<K, V> getFolderNameExtractor() {
		return new KeyFolderNameExtractor<K, V>();
	}

	public interface FolderNameExtractor<K, V> {
		public String extractFolderName(K key, V value);
	}

	private static class KeyFolderNameExtractor<K, V> implements FolderNameExtractor<K, V> {
		public String extractFolderName(K key, V value) {
			return key.toString();
		}
	}

}