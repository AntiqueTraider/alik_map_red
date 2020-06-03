package ru.mapreduce.oab;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import ru.mapreduce.oab.mapreduce.calls.CallMapper;
import ru.mapreduce.oab.mapreduce.calls.CallReducer;
import ru.mapreduce.oab.mapreduce.result.SubAndNameReducer;
import ru.mapreduce.oab.mapreduce.result.full_name.NameBanMapper;
import ru.mapreduce.oab.mapreduce.result.full_name.NameSubMapper;
import ru.mapreduce.oab.mapreduce.result.sub_key.SubNameMapper;
import ru.mapreduce.oab.mapreduce.result.sub_key.SubTableMapper;


public class Main {

    public static final Logger LOG = Logger.getLogger(Main.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {


        String stage = "";
        String queueName ="";
        String tasks = "";
        String projectName = "";
        String pathCdrInput = "";
        String pathCdrOutput = "";
        String pathDimBanInput = "";
        String pathDimSubsInput = "";
        String pathDimBanSubsOut = "";
        String resOutPath = "";
        Configuration conf;

        for (String arg : args) {
            String argSplit[] = arg.split("=");
            if (argSplit.length >= 2) {

                cmdParse currentCmd = cmdParse.valueOf(argSplit[0]);

                switch (currentCmd) {
                    case QUEUE_NAME:
                        queueName = argSplit[1];
                        break;
                    case STAGE:
                        stage = argSplit[1];
                        break;
                    case QUANTITY_TASKS:
                        tasks = argSplit[1];
                        break;
                    case PROJECT_NAME:
                        projectName = argSplit[1];
                        break;
                    case PATH_CDR_INPUT:
                        pathCdrInput = argSplit[1];
                        break;
                    case PATH_CDR_OUTPUT:
                        pathCdrOutput = argSplit[1];
                        break;
                    case PATH_DIM_BAN_INPUT:
                        if (argSplit.length > 2) {
                            pathDimBanInput = argSplit[1] + "=" + argSplit[2];
                        } else {
                            pathDimBanInput = argSplit[1];
                        }
                        break;
                    case PATH_DIM_SUBSCRIBER_INPUT:
                        if (argSplit.length > 2) {
                            pathDimSubsInput = argSplit[1] + "=" + argSplit[2];
                        } else {
                            pathDimSubsInput = argSplit[1];
                        }
                        break;
                    case PATH_DIM_BAN_SUBSCRIBER_OUTPUT:
                        pathDimBanSubsOut = argSplit[1];
                        break;
                    case RES_OUTPUT_PATH:
                        resOutPath = argSplit[1];
                        break;
                    default:
                        break;
                }
            }
        }

        System.out.println("args");
        System.out.println("stage = " + stage);
        System.out.println("tasks = " + tasks);
        System.out.println("projectName = " + projectName);
        System.out.println("pathCdrInput = " + pathCdrInput);
        System.out.println("pathCdrOutput = " + pathCdrOutput);
        System.out.println("pathDimBanInput = " + pathDimBanInput);
        System.out.println("pathDimSubsInput = " + pathDimSubsInput);
        System.out.println("pathDimBanSubsOut = " + pathDimBanSubsOut);
        System.out.println("resOutPath = " + resOutPath);



        switch (stage){
            case "1":
                conf = new Configuration();
                conf.set("mapreduce.job.queuename",queueName);

                Job job = Job.getInstance(conf);
                job.setJarByClass(Main.class);

                job.setInputFormatClass(SequenceFileInputFormat.class);
                job.setOutputFormatClass(SequenceFileOutputFormat.class);

                job.setMapperClass(CallMapper.class);
                job.setReducerClass(CallReducer.class);

                FileInputFormat.addInputPath(job, new Path(pathCdrInput)); // путь до входных данных
                FileOutputFormat.setOutputPath(job, new Path(pathCdrOutput)); // путь до результата


                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);

                job.setOutputKeyClass(LongWritable.class);
                job.setOutputValueClass(Text.class);

                job.setNumReduceTasks(Integer.valueOf(tasks));

                System.exit(job.waitForCompletion(true) ? 0 : 1);
                break;
            case "2":
                conf = new Configuration();
                conf.set("mapreduce.job.queuename",queueName);

                Job job_second = Job.getInstance(conf);
                job_second.setJarByClass(Main.class);

                job_second.setReducerClass(SubAndNameReducer.class);

                MultipleInputs.addInputPath(job_second, new Path(pathDimBanInput), SequenceFileInputFormat.class, NameBanMapper.class);
                MultipleInputs.addInputPath(job_second, new Path(pathDimSubsInput), SequenceFileInputFormat.class, NameSubMapper.class);
                FileOutputFormat.setOutputPath(job_second, new Path(pathDimBanSubsOut)); // путь до результата

                job_second.setOutputFormatClass(SequenceFileOutputFormat.class);

                job_second.setOutputKeyClass(LongWritable.class);
                job_second.setOutputValueClass(Text.class);

                job_second.setNumReduceTasks(Integer.valueOf(tasks));

                System.exit(job_second.waitForCompletion(true) ? 0 : 1);
                break;
            case "3":
                conf = new Configuration();
                conf.set("mapreduce.job.queuename",queueName);


                Job job3 = Job.getInstance(conf);
                job3.setJarByClass(Main.class);

                job3.setReducerClass(SubAndNameReducer.class);

                job3.setOutputKeyClass(LongWritable.class);
                job3.setOutputValueClass(Text.class);

                job3.setNumReduceTasks(Integer.valueOf(tasks));

                MultipleInputs.addInputPath(job3, new Path(pathCdrOutput), SequenceFileInputFormat.class, SubTableMapper.class);
                MultipleInputs.addInputPath(job3, new Path(pathDimBanSubsOut), SequenceFileInputFormat.class, SubNameMapper.class);

                job3.setOutputFormatClass(TextOutputFormat.class);

                FileOutputFormat.setOutputPath(job3, new Path(resOutPath));

                System.exit(job3.waitForCompletion(true) ? 0 : 1);
                break;
        }
    }

    enum cmdParse {
        QUEUE_NAME,
        STAGE,
        QUANTITY_TASKS,
        PROJECT_NAME,
        PATH_CDR_INPUT,
        PATH_CDR_OUTPUT,
        PATH_DIM_BAN_INPUT,
        PATH_DIM_SUBSCRIBER_INPUT,
        PATH_DIM_BAN_SUBSCRIBER_OUTPUT,
        RES_OUTPUT_PATH
    }
}

