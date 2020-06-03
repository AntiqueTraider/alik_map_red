package ru.mapreduce.oab.mapreduce.result.full_name;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NameBanMapper extends Mapper <LongWritable, Text, LongWritable, Text>  {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String line = value.toString();

        String [] fields = line.split("\u0001");

        Long banKey = Long.parseLong(fields[0]);

        String castFullName = fields[14];

        context.write(new LongWritable(banKey), new Text(castFullName));

    }
}
