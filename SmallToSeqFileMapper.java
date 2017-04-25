package com.demo.sequence;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by hadoop on 24/4/17.
 */
public class SmallToSeqFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {

    private Text fileName;

    @Override
    public void setup(Context context){
        InputSplit inputSplit = context.getInputSplit();
        Path path = ((FileSplit) inputSplit).getPath();
        fileName = new Text(path.toString());
        System.out.println("Retrive the file name "+path.toString());
    }

    @Override
    public void map(NullWritable nullWritable, BytesWritable bytesWritable, Context context) throws IOException, InterruptedException {
        System.out.println("write the contents to the seq file with key as file name and value as byteswritable");
        context.write(fileName, bytesWritable);
        System.out.println("added the contents of the input split to seq file");
    }

}
