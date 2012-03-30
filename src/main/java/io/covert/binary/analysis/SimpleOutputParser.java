package io.covert.binary.analysis;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class SimpleOutputParser implements OutputParser<Text, Text> {

	List<Entry<Text, Text>> results = new LinkedList<Map.Entry<Text,Text>>();
	
	@Override
	public void parse(Text key, BytesWritable val, byte[] stdOut, byte[] stdErr) {
		results.clear();
		results.add(new io.covert.util.Entry<Text, Text>(key, new Text(stdOut)));
	}
	
	public void parse(Text key, BytesWritable val, java.io.ByteArrayOutputStream stdOut, java.io.ByteArrayOutputStream stdErr) 
	{
		results.clear();
		results.add(new io.covert.util.Entry<Text, Text>(key, new Text(stdOut.toByteArray())));
	}
	
	@Override
	public void parse(Text key, BytesWritable val, File file) 
	{
		throw new NotImplementedException();
	}
	
	public java.util.Collection<java.util.Map.Entry<Text,Text>> getResults() 
	{
		return results;
	}
}
