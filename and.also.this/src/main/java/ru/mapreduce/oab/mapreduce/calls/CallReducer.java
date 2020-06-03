package ru.mapreduce.oab.mapreduce.calls;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CallReducer extends Reducer <Text ,Text , LongWritable, Text>{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

        /*
          00:00:00-03:59:59 - ночь;
          04:00:00-11:59:59 - утро;
          12:00:00-16:59:59 - день;
          17:00:00-23:59:59 - вечер.
         */
        int countVoiceMorning = 0;
        int countMessageMorning = 0;

        int countVoiceAfternoon = 0;
        int countMessageAfternoon = 0;

        int countVoiceEvening = 0;
        int countMessageEvening = 0;

        int countVoiceNight = 0;
        int countMessageNight = 0;

        for (Text value : values){

            String time = value.toString();

            int hour = Integer.parseInt(time.substring(0,2));

            char typeOfTransaction=time.charAt(6);

           // System.out.println(hour);

            if (hour >= 0 && hour < 4){
                switch (typeOfTransaction){
                    case 'V' :
                        countVoiceNight++;
                        break;

                    case 'S' :
                        countMessageNight++;
                        break;

                    default :
                        break;
                }
            }
            else if(hour >= 4 && hour < 12){
                switch (typeOfTransaction){
                    case 'V' :
                        countVoiceMorning++;
                        break;

                    case 'S' :
                        countMessageMorning++;
                        break;

                    default :
                        break;
                }

            }
            else if(hour >= 12 && hour < 17){
                switch (typeOfTransaction){
                    case 'V' :
                        countVoiceAfternoon++;
                        break;

                    case 'S' :
                        countMessageAfternoon++;
                        break;

                    default :
                        break;
                }

            }
            else if(hour >= 17 && hour < 24){
                switch (typeOfTransaction){
                    case 'V' :
                        countVoiceEvening++;
                        break;

                    case 'S' :
                        countMessageEvening++;
                        break;

                    default :
                        break;
                }
            }
        }
        String [] buf = key.toString().split("[+]");

        if (buf[0].charAt(0)=='9') {

            Long number = Long.parseLong(buf[0]);

            context.write(new LongWritable(number), new Text(buf[1] + "|" + countVoiceMorning + "|" + countMessageMorning + "|" +
                    countVoiceAfternoon + "|" + countMessageAfternoon + "|" +
                    countVoiceEvening + "|" + countMessageEvening + "|" +
                    countVoiceNight + "|" + countMessageNight));
        }
    }
}