package com.demo.sequence;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Created by hadoop on 24/4/17.
 */
public class CustomFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        System.out.println("Creating custom file record reader");
        CustomFileRecordReader customFileRecordReader = new CustomFileRecordReader();
        customFileRecordReader.initialize(inputSplit, taskAttemptContext);
        System.out.println("Returning the custom record reader");
        return customFileRecordReader;
    }

    @Override
    public boolean isSplitable(JobContext context, Path file){
        return false;
    }
}
