package com.demo.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by hadoop on 24/4/17.
 */
public class CustomFileRecordReader extends RecordReader {

    private FileSplit fileSplit;
    private Configuration configuration;
    private BytesWritable bytesWritable = new BytesWritable();
    private boolean processed = false;
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit)inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if (!processed){
            System.out.println("Reading the contents of the files");
            byte[] contents = new byte[(int)fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fileSystem = file.getFileSystem(configuration);
            FSDataInputStream fsDataInputStream = null;
            try {
                fsDataInputStream = fileSystem.open(file);
                IOUtils.readFully(fsDataInputStream, contents, 0, contents.length);
                bytesWritable.set(contents, 0, contents.length);
                System.out.println("Added the contents to byteswritable "+contents.length);
            }finally {
                IOUtils.closeStream(fsDataInputStream);
            }
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {

    }
}
