package hadoop.study.book.ch06.v1;

// == MaxTemperatureReducerTestV1
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;

public class MaxTemperatureReducerTest {

	// vv MaxTemperatureReducerTestV1
	@Test
	public void returnsMaximumIntegerInValues() throws IOException, InterruptedException {
		new ReduceDriver<Text, IntWritable, Text, IntWritable>()
			.withReducer(new MaxTemperatureReducer())
			.withInput(new Text("1950"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
			.withInput(new Text("1951"), Arrays.asList(new IntWritable(25), new IntWritable(15)))
			.withOutput(new Text("1950"), new IntWritable(10))
			.withOutput(new Text("1951"), new IntWritable(25))
			.runTest();
	}
	// ^^ MaxTemperatureReducerTestV1
}
