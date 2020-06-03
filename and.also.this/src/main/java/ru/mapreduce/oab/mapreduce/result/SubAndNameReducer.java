package ru.mapreduce.oab.mapreduce.result;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


public class SubAndNameReducer extends Reducer<LongWritable,Text,LongWritable,Text> {

    /**
     * Результаты работы мапперов для сязывания по BAN_KEY и SUBS_KEY
     * свертываются одним методом reduce
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        ArrayList<String> lines = new ArrayList<>();

        int i=0;
        for (Text value : values){
            i++;
            lines.add(value.toString());
        }

        if (i>=2) {
            String line = String.join("|", lines);

            context.write(key, new Text(line));
        }
    }

}
