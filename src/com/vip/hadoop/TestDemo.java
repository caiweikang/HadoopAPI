package com.vip.hadoop;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.ThrowsException;

import sun.nio.ch.IOUtil;

public class TestDemo {
	
	@Test
	public void test_connect() throws Exception { 
		
		Configuration conf = new Configuration();
		//FileSystem是hadoop文件系统的抽象类
		//HDFS只是hadoop文件系统的一个实现类
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.154.129:9000"), conf);
		fs.close();
	}
	
	@Test
	public void test_mkdir() throws Exception {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.154.129:9000"), conf);
		fs.mkdirs(new Path("/park02"));
		fs.close();
	}
	
	@Test
	public void test_copyToLocal() throws Exception { 
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.154.129:9000"), conf);
		//method1: fs.copyToLocalFile(new Path("/park01/1.txt"), new Path("1.txt"));
		InputStream inputStream = fs.open(new Path("/park01/1.txt"));
		OutputStream outputStream = new FileOutputStream(new File("2.txt"));
		IOUtils.copyBytes(inputStream, outputStream, conf);
		fs.close();
	}
	@Test
	public void test_copyFromLocal() throws Exception { 
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.154.129:9000"), conf);
		fs.copyFromLocalFile(new Path("2.txt"), new Path("/park01/2.txt"));
		fs.close();
	}
}
