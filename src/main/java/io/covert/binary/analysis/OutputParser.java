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
