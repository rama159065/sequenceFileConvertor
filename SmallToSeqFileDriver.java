package com.demo.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by hadoop on 24/4/17.
 */
public class SmallToSeqFileDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new SmallToSeqFileDriver(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Configuration configuration = new Configuration();
        Job job = new Job(configuration, "Merge and create sequence Files");
        job.setJobName("SmallToSeqFiles");
        job.setJarByClass(SmallToSeqFileDriver.class);
        job.setMapperClass(SmallToSeqFileMapper.class);
        job.setInputFormatClass(CustomFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int returnValue = job.waitForCompletion(true) ? 0:1;
        System.out.println("job.isSuccessful " + job.isSuccessful());
        return returnValue;
    }
}
