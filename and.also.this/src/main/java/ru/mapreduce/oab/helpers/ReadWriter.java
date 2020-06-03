package ru.mapreduce.oab.helpers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ReadWriter {
 private Pair pair;

 public List<Pair<LongWritable, Text>> read(String s) throws IOException {
  Stream <String> stream = Files.lines(Paths.get(s));
  ArrayList<Pair<LongWritable,Text>> fromFile = new ArrayList<>();

  ArrayList<String> buffer = (ArrayList<String>) stream.collect(Collectors.toList());
  int i=0;
  for (String str : buffer){
      fromFile.add(new Pair<>(new LongWritable(i++), new Text(str)));
  }
  return fromFile;
 }

 public void writeCall(String s, List<Pair<LongWritable, Text>> run) throws IOException {
  ArrayList<String> buffer = new ArrayList<>();

  for(Pair<LongWritable, Text> p : run){
       buffer.add(p.getFirst().toString()+"|"+p.getSecond().toString());
  }
  Files.write(Paths.get(s), buffer, StandardCharsets.UTF_8);

 }

 public void writeFullName(String s, List<Pair<LongWritable, Text>> run) throws IOException {
  ArrayList<String> buffer = new ArrayList<>();

  for(Pair<LongWritable, Text> p : run){
   buffer.add(p.getFirst().toString()+"|"+p.getSecond().toString());
  }
  Files.write(Paths.get(s), buffer, StandardCharsets.UTF_8);
 }
}
