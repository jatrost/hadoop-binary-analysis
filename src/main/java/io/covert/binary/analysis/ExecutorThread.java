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

import io.covert.util.Utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;

public class ExecutorThread extends Thread {

	CommandLine cmdLine;
	DefaultExecuteResultHandler resultHandler;
	ExecuteWatchdog watchdog;
	Executor executor;
	
	volatile boolean keepRnning = true;
	
	ByteArrayOutputStream stdOut;
	ByteArrayOutputStream stdErr;
	PumpStreamHandler streamHandler;
	int exitCode = -1;
	ExecuteException execExcep;
	
	boolean processDestroyed = false;
	boolean processStarted = false;
	
	public ExecutorThread(String prog, String[] args, Map<String, Object> substitutionMap, int[] exitCodes, long timeoutMS, File workingDirectory) throws ExecuteException, IOException
	{
		cmdLine = new CommandLine(prog);
		cmdLine.addArguments(args);
		cmdLine.setSubstitutionMap(substitutionMap);

		resultHandler = new DefaultExecuteResultHandler();

		watchdog = new ExecuteWatchdog(60*1000);
		executor = new DefaultExecutor();
		
		stdOut = new ByteArrayOutputStream();
		stdErr = new ByteArrayOutputStream();
		
		streamHandler = new PumpStreamHandler(stdOut, stdErr);
		executor.setStreamHandler(streamHandler);
		executor.setWorkingDirectory(workingDirectory);
		executor.setExitValues(exitCodes);
		executor.setWatchdog(watchdog);
	}
	
	@Override
	public void run() {
		
		try {
			executor.execute(cmdLine, resultHandler);
			processStarted = true;
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		
		while(keepRnning)
		{
			if(resultHandler.hasResult())
			{
				exitCode = resultHandler.getExitValue();
				execExcep = resultHandler.getException();
				return;
			}
			else
			{
				Utils.sleep(100);
			}
		}
		
		if(resultHandler.hasResult())
		{
			exitCode = resultHandler.getExitValue();
			execExcep = resultHandler.getException();
			return;
		}
		
		watchdog.destroyProcess();
		processDestroyed = true;
	}
	
	public void stopRunning()
	{
		keepRnning = false;
	}
	
	public boolean isProcessDestroyed() {
		return processDestroyed;
	}
	
	public boolean isProcessStarted() {
		return processStarted;
	}
	
	public int getExitCode() {
		return exitCode;
	}
	
	public ExecuteException getExecuteException() {
		return execExcep;
	}
	
	public ByteArrayOutputStream getStdOut() {
		return stdOut;
	}
	
	public ByteArrayOutputStream getStdErr() {
		return stdErr;
	}
	
	public static void main(String[] args) throws Exception {
		
		HashMap<String, Object> substitutionMap = new HashMap<String, Object>();
		substitutionMap.put("file", "/home/jtrost/workspace/hadoop-binary-analysis/pom.xml");
		
		ExecutorThread e = new ExecutorThread("sha1sum", new String[]{"${file}"}, substitutionMap, new int[]{0,1}, Long.MAX_VALUE, new File("/tmp"));
		e.start();
		e.join();
		
		System.out.println("exitCode = "+e.getExitCode());
		System.out.println("exitCode = "+e.getExecuteException());
		
		if(e.isProcessStarted() && !e.isProcessDestroyed())
		{
			System.out.println("stdout: "+new String(e.getStdOut().toByteArray()));
			System.out.println("stderr: "+new String(e.getStdErr().toByteArray()));
		}
		else if(e.isProcessStarted())
		{
			System.out.println("Process was launched, but process destroyed");
		}
		else
		{
			System.out.println("Process not launched");
		}
	}
}
