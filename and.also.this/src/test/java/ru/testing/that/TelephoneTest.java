package ru.testing.that;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import org.junit.Before;
import org.junit.Test;
import ru.mapreduce.oab.helpers.ReadWriter;
import ru.mapreduce.oab.mapreduce.calls.CallMapper;
import ru.mapreduce.oab.mapreduce.calls.CallReducer;
import ru.mapreduce.oab.mapreduce.result.SubAndNameReducer;
import ru.mapreduce.oab.mapreduce.result.full_name.NameBanMapper;
import ru.mapreduce.oab.mapreduce.result.full_name.NameSubMapper;
import ru.mapreduce.oab.mapreduce.result.sub_key.SubNameMapper;
import ru.mapreduce.oab.mapreduce.result.sub_key.SubTableMapper;

import java.io.IOException;

public class TelephoneTest {


    private Pair pair;
    private ReadWriter readWriter;
    private MapReduceDriver<LongWritable, Text, Text, Text, LongWritable, Text> mapReduceDriver;
    private MultipleInputsMapReduceDriver <LongWritable,Text,LongWritable,Text> firstDriver;
    private MultipleInputsMapReduceDriver <LongWritable,Text,LongWritable,Text> secondDriver;
    private String resourcePath = "src\\main\\resources\\";
    private NameBanMapper nameBanMapper = new NameBanMapper();//инициализация 1-го маппера 2-й стадии
    private NameSubMapper nameSubMapper = new NameSubMapper();//инициализация 2-го маппера 2-й стадии
    private SubTableMapper subTableMapper = new SubTableMapper();//инициализация 1-го маппера 3-й стадии
    private SubNameMapper subNameMapper = new SubNameMapper();//инициализация 2-го маппера 3-й стадии

    @Before
    public void setUp(){
        CallMapper callMapper = new CallMapper();//инициализация маппера 1-й стадии
        CallReducer callReducer = new CallReducer();//инициализация редьюсера 1-й стадии


        SubAndNameReducer subAndNameReducer = new SubAndNameReducer();//инициализация редьюсера 2-й и 3-й стадий


        readWriter = new ReadWriter();

        mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, Text, LongWritable, Text>(); //входной файл один
        mapReduceDriver.setMapper(callMapper); //привязываем к драйверу маппер и редьюсер
        mapReduceDriver.setReducer(callReducer);

        firstDriver = new MultipleInputsMapReduceDriver<>(); //входных файлов 2
        firstDriver.addMapper(nameBanMapper); //привязываем к драйверу мапперы и редьюсер
        firstDriver.addMapper(nameSubMapper);
        firstDriver.setReducer(subAndNameReducer);

        secondDriver = new MultipleInputsMapReduceDriver<>(); //входных файлов 2
        secondDriver.addCacheFile(resourcePath+"numbers.csv"); // добавляем файл-справочник, для проверки номеров

        secondDriver.addMapper(subTableMapper); //привязываем к драйверу мапперы и редьюсер
        secondDriver.addMapper(subNameMapper);
        secondDriver.setReducer(subAndNameReducer);


    }



    @Test
    public void testCallMapReduce () throws IOException {
        try {
            mapReduceDriver.addAll(readWriter.read(resourcePath + "call_input.csv"));
            //add Cache file
            readWriter.writeCall(resourcePath + "call_output.txt", mapReduceDriver.run());

        }
        catch(Exception exc){
            System.out.println(exc);
            exc.getStackTrace();
        }
        /*Stream<String> stream = Files.lines(Paths.get(resourcePath+"call_output.txt"));
        stream.forEach(p->System.out.println(p));*/

    }
    @Test
    public void testFullNameMapReduce () throws IOException {
        try {
            // mapReduceDriver.addAll(readWriter.read(resourcePath + "call_input.csv"));
            // readWriter.writeWc(resourcePath + "call_output.txt", mapReduceDriver.run());

            firstDriver.addAll(nameBanMapper, readWriter.read(resourcePath+"DIM_BAN.csv"));
            firstDriver.addAll(nameSubMapper, readWriter.read(resourcePath+"DIM_SUBSCRIBER.csv"));
            readWriter.writeFullName(resourcePath+"full_name_output.txt", firstDriver.run());
        }
        catch(Exception exc){
            System.out.println(exc);
            exc.getStackTrace();
        }
    }
    @Test
    public void testSubKeyMapReduce () throws IOException {
        try {

            secondDriver.addAll(subTableMapper, readWriter.read(resourcePath+"call_output.txt"));
            secondDriver.addAll(subNameMapper, readWriter.read(resourcePath+"full_name_output.txt"));
            readWriter.writeFullName(resourcePath+"wc_output.txt", secondDriver.run());
        }
        catch(Exception exc){
            System.out.println(exc);
            exc.getStackTrace();
        }
    }

}
