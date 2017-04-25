package com.demo.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;

/**
 * Created by hadoop on 24/4/17.
 */
public class SequenceFileKeyExtractor {
    public static void main(String[] args) throws Exception {
        System.out.println("Testing SequenceFileKeyExtractor");
        Configuration conf = new Configuration();
        Path path = new Path(args[0]);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(conf, Reader.file(path));
            Text key = new Text();
            while (reader.next(key)) { System.out.println(key);
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
