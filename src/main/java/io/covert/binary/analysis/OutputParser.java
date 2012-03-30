package io.covert.binary.analysis;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Collection;
import java.util.Map.Entry;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

public interface OutputParser<K, V> {
	
	public void parse(Text key, BytesWritable val, ByteArrayOutputStream stdOut, ByteArrayOutputStream stdErr);
	
	public void parse(Text key, BytesWritable val, byte[] stdOut, byte[] stdErr);
	
	public void parse(Text key, BytesWritable val, File file);
	
	public Collection<Entry<K, V>> getResults();
}
