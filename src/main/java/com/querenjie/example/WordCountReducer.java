package com.querenjie.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    /**
     * reduce方法提供给reduce task进程来调用
     * reduce task会将shuffle阶段分发过来的大量kv数据对进行聚合，聚合的机制是相同key的kv对聚合为一组
     * 然后reduce task对每一组聚合kv调用一次我们自定义的reduce方法
     * 比如：<hello,1><hello,1><hello,1><tom,1><tom,1><tom,1>
     * hello组会调用一次reduce方法进行处理，tom组也会调用一次reduce方法进行处理
     * 调用时传递的参数：
     * 		key：一组kv中的key
     * 		values：一组kv中所有value的迭代器
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //定义一个计数器
        long count = 0L;
        //通过value这个迭代器，遍历这一组kv中所有的value，进行累加
        for (LongWritable longValue : values) {
            count += longValue.get();
        }
        //输出这个单词的统计结果
        context.write(key, new LongWritable(count));
    }
}
