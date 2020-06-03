package ru.mapreduce.oab.mapreduce.calls;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class CallMapper extends Mapper<LongWritable,Text,Text,Text>{

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


        /*
         *С помощью разделителя строка, содержащая все поля,
         * разбиватся на массив из строк, каждая из которых соднржит поле
         * */
        String line = value.toString();
        String[] fields = line.split("[|]");


        if (fields.length >= 33) {
            String subs_key = fields[1];//номер абонента

            /*
             * Запись в разные переменные времени и даты, и преобразование
             *  каждого элемента в int для проверки корректности даты
             * */
            String date = fields[2].substring(0, 8);

            int year = Integer.parseInt(date.substring(0, 4));
            int month = Integer.parseInt(date.substring(4, 6));
            int day = Integer.parseInt(date.substring(6));

            String time = fields[2].substring(8);

            int hour = Integer.parseInt(time.substring(0, 2));
            int minutes = Integer.parseInt(time.substring(2, 4));
            int seconds = Integer.parseInt(time.substring(4));


            String basic_service_type = fields[32];//  тип транзакции



            /*
             * Проверка необходимых типов транзакций и корректности даты
             * Определяем точное время сейчас и при сравнении определяем,
             * коректно ли указанное время транзакции
             * */
            Date now = new Date();
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Russia/Moscow"));
            cal.setTime(now);
            if (basic_service_type.matches("[VS]") &&
                    (year < cal.get(Calendar.YEAR) ||
                            (year == cal.get(Calendar.YEAR) && month < cal.get(Calendar.MONTH) ||
                                    (year == cal.get(Calendar.YEAR) && month == cal.get(Calendar.MONTH) && day < cal.get(Calendar.DAY_OF_MONTH) ||
                                            (year == cal.get(Calendar.YEAR) && month == cal.get(Calendar.MONTH) && day == cal.get(Calendar.DAY_OF_MONTH) && hour < cal.get(Calendar.HOUR_OF_DAY) ||
                                                    (year == cal.get(Calendar.YEAR) && month == cal.get(Calendar.MONTH) && day == cal.get(Calendar.DAY_OF_MONTH) && hour < cal.get(Calendar.HOUR_OF_DAY) && minutes < cal.get(Calendar.MINUTE) ||
                                                            (year == cal.get(Calendar.YEAR) && month == cal.get(Calendar.MONTH) && day == cal.get(Calendar.DAY_OF_MONTH) && hour < cal.get(Calendar.HOUR_OF_DAY) && minutes < cal.get(Calendar.MINUTE) && seconds < cal.get(Calendar.SECOND)))))))) {


                context.write(new Text(subs_key + "+" + date), new Text(time + basic_service_type));
            }
        }
    }


}
