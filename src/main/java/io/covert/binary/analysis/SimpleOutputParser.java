/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.covert.binary.analysis;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

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
		throw new RuntimeException("Not implemented");
	}
	
	public java.util.Collection<java.util.Map.Entry<Text,Text>> getResults() 
	{
		return results;
	}
}
