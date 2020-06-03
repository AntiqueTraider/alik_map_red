package ru.mapreduce.oab.mapreduce.result.full_name;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class NameSubMapper extends Mapper <LongWritable, Text, LongWritable, Text>{

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String line = value.toString();

        String [] fields = line.split("\u0001");

        Long banKey = Long.parseLong(fields[0]);

        String subsKey= fields[1];

        context.write(new LongWritable(banKey), new Text(subsKey));

    }
}