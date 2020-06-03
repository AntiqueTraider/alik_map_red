package ru.mapreduce.oab.mapreduce.result.sub_key;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class SubTableMapper extends Mapper<LongWritable, Text, LongWritable, Text>  {


    @Override
    public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException{
        String [] line = value.toString().split("[|]");
        //context.write(new LongWritable(Long.parseLong(line[0])), new Text(key.toString()+"|"+line[1]));

        context.write(new LongWritable(Long.parseLong(line[0])), new Text(value.toString().substring(line.length)));
    }

}
