package ru.testing.that;
/*
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mrunit.types.Pair;
import ru.at_consulting.bigdata.staging.dimBan.DimBanMapper;
import ru.at_consulting.bigdata.staging.match.MatchMapper;
import ru.at_consulting.bigdata.staging.match.MatchReducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import ru.at_consulting.bigdata.staging.cdr.CallingMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import ru.at_consulting.bigdata.staging.cdr.CallingReducer;
import ru.at_consulting.bigdata.staging.cdr.ReadWriter;
import ru.at_consulting.bigdata.staging.subscribe.SubscribeMapper;
import ru.at_consulting.bigdata.staging.subscribe.SubscribeReducer;
import ru.at_consulting.bigdata.staging.wc.CountMapper;
import ru.at_consulting.bigdata.staging.wc.CountReducer;

import javax.management.monitor.CounterMonitor;
import java.util.List;


public class TestCallCounter {
     private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;
     private MultipleInputsMapReduceDriver<LongWritable, Text, Text, Text> driver;

     private ReadWriter readWriter ;
     private String resourcePath = "src\\main\\resources\\";

     public void setUp(){
         CallingMapper mapper = new CallingMapper(); //инициализация маппера
         CallingReducer reducer = new CallingReducer(); //инициализация редьюсера

         mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, Text, Text>(); //если файл один
         mapReduceDriver.setMapper(mapper); //привязываем к драйверу маппер и редьюсер
         mapReduceDriver.setReducer(reducer);
}


     @Test
     public void testWordCount(){
         readWriter = new ReadWriter(); // вспомогательный класс для считывания файлов
         CountMapper mapper = new CountMapper(); //инициализация маппера
         CountReducer reducer = new CountReducer(); //инициализация редьюсера

         MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver = new MapReduceDriver<>();
         mapReduceDriver.setMapper(mapper); //привязываем к драйверу маппер и редьюсер
         mapReduceDriver.setReducer(reducer);

         try {
             mapReduceDriver.addAll(readWriter.read(resourcePath + "call_input.csv"));
             readWriter.writeWc(resourcePath + "call_output.txt", mapReduceDriver.run());
         }
         catch(Exception exc){
             exc.getStackTrace();
         }

     }


}
*/