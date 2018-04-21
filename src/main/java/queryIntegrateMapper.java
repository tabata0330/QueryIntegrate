import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

public class queryIntegrateMapper extends Mapper<LongWritable, Text, Text, IntArrayWritable> {
    private IntArrayWritable outValue;
    private Text outKeys = new Text();


    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
        // 入力から文章を取り出してスペース区切りにする

        boolean flag = false;
        ArrayList<String> list = new ArrayList<>();
        for (String word : inputValue.toString().trim().split(" ")) {
            //属性の該当する数字群だったら
            if (word.equals("number")){
                flag = !flag;
                continue;
            }
            //数字をlistに代入
            list.add(word);
        }
        //numberだったらValueを更新する
        if(flag){
            //配列を作るとき//
            //listの要素数分の配列を作る
            IntWritable outValues[] = new IntWritable[list.size()];
            int i = 0;
            //listの要素それぞれについて配列に突っ込む
            for(String num : list){
                IntWritable tmp = new IntWritable();
                tmp.set(Integer.parseInt(num));
                outValues[i] = tmp; //value
                i++;
            }
            outValue = new IntArrayWritable(outValues);
            //ここまで//
            //文字列とするとき//
//            String value = new String();
//            for(String num : list){
//                value += num + " ";
//            }
//            value = value.trim();
//            outValues.set(value);
            //ここまで//
        }else{
            //検索結果の時
//            Text outKeys[] = new Text[list.size()];
//            int i = 0;
//            for(String c : list){
//                outKeys[i].set(c);
//                i++;
//            }
//            key = key.trim().substring(0, key.length() - 1);
            String key = new String();
            //検索結果それぞれについて
            for(String c: list){
                //文字列型に連結する
                key += c + " ";
            }
            key = key.trim();
            outKeys.set(key);
//            outKey = new ArrayWritable(Text.class, outKeys);
            context.write(outKeys, outValue);
        }

    }
}