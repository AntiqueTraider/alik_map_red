package ru.mapreduce.oab.mapreduce.result.sub_key;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class SubNameMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{

                String [] line = value.toString().split("[|]");

                if (line.length == 3) {
                        context.write(new LongWritable(Long.parseLong(line[2])), new Text(line[0]+"|"+line[1]));
                }
        }
}
