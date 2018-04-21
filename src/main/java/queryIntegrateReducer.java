import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class queryIntegrateReducer extends Reducer<Text, IntArrayWritable, Text, Text> {
    //    private IntWritable outputValue = new IntWritable();
    private ArrayList<ArrayList<String>> keys = new ArrayList<>();
    private ArrayList<ArrayList<Integer>> values = new ArrayList<>();
    private boolean flag = false;
    private int lim = 7;
    @Override
    public void reduce(Text inputKey, Iterable<IntArrayWritable> inputValue, Context context) throws IOException, InterruptedException {
//        context.setStatus("keys:" + keys);
//        context.setStatus("values:" + values);
        int num = values.size();
        //初回
        if(num == 0){
            //検索結果を配列に格納
            ArrayList<String> key_tmp = new ArrayList<>();
            for(String key: inputKey.toString().split(" ")){
                key_tmp.add(key);
            }
            //valueを配列に格納
            ArrayList<Integer> value_tmp = new ArrayList<>();
            for(IntArrayWritable values: inputValue){
                for(Writable value: values.get()){
                    IntWritable value0 = (IntWritable) value;
                    int value_0 = value0.get();
                    value_tmp.add(value_0);
                }
            }
            //valueの要素数が上限と等しかったら
            if(value_tmp.size() == lim){
                //書き出す
                String outKey = new String();
                String outValue = new String();
                //検索結果は文字列に連結
                for(String key: key_tmp){
                    outKey += key + " ";
                }
                //valueも文字列として連結
                for(Integer value: value_tmp){
                    outValue += value.toString() + " ";
                }
                outKey = outKey.trim();
                outValue = outValue.trim();
                //Textにして書き出す
                Text outKey_t = new Text();
                Text outValue_t = new Text();
                outKey_t.set(outKey);
                outValue_t.set(outValue);
                context.write(outKey_t, outValue_t);
            }else{
                //valuesとkeysとして保管
                values.add(value_tmp);
                keys.add(key_tmp);
            }
        }else{
            //初回じゃなかったら
            ArrayList<Integer> inputvalues = new ArrayList<>();
            ArrayList<String> inputkeys = new ArrayList<>();
            for(String key: inputKey.toString().split(" ")){
                inputkeys.add(key);
            }
            for(IntArrayWritable values: inputValue){
                for(Writable value: values.get()){
                    IntWritable value0 = (IntWritable) value;
                    int value_0 = value0.get();
                    inputvalues.add(value_0);
                }
            }
            if(inputvalues.size() == lim){
                String outKey = new String();
                String outValue = new String();
                for(String key: inputkeys){
                    outKey += key + " ";
                }
                for(Integer value: inputvalues){
                    outValue += value.toString() + " ";
                }
                outKey = outKey.trim();
                outValue = outValue.trim();
                Text outKey_t = new Text();
                Text outValue_t = new Text();
                outKey_t.set(outKey);
                outValue_t.set(outValue);
                context.write(outKey_t, outValue_t);
            }else {
                for (int i = 0; i < num; i++) {
                    ArrayList<Integer> value = values.get(i);
                    ArrayList<String> key = keys.get(i);
                    boolean same_flag = true;
//                    Text keyout = new Text();
//                    Text valueout = new Text();
//                    keyout.set("-----same_flag_before-----");
//                    valueout.set(String.valueOf(same_flag));
//                    context.write(keyout, valueout);
//                    keyout.set("inputkeys");
//                    valueout.set(inputkeys.toString());
//                    context.write(keyout, valueout);
//                    keyout.set("inputvalues");
//                    valueout.set(inputvalues.toString());
//                    context.write(keyout, valueout);
//                    keyout.set("key");
//                    valueout.set(key.toString());
//                    context.write(keyout, valueout);
//                    keyout.set("value");
//                    valueout.set(value.toString());
//                    context.write(keyout, valueout);
                    for (int j = 0; j < value.size(); j++) {
                        for (int k = 0; k < inputvalues.size(); k++) {
//                            keyout.set("value.get(j)");
//                            valueout.set(value.get(j).toString());
//                            context.write(keyout, valueout);
//                            keyout.set("inputvalues.get(k)");
//                            valueout.set(inputvalues.get(k).toString());
//                            context.write(keyout, valueout);
                            if (value.get(j) == inputvalues.get(k)) {
//                                keyout.set("key.get(j)");
//                                valueout.set(key.get(j).toString());
//                                context.write(keyout, valueout);
//                                keyout.set("inputkeys.get(k)");
//                                valueout.set(inputkeys.get(k).toString());
//                                context.write(keyout, valueout);
                                if (!key.get(j).equals(inputkeys.get(k))) {
//                                    keyout.set("IN");
//                                    valueout.set("しました");
//                                    context.write(keyout, valueout);
                                    same_flag = !same_flag;
                                    break;
                                }
                            }
                        }
                        if(!same_flag){
                            break;
                        }
                    }
//                    keyout.set("-----same_flag_after-----");
//                    valueout.set(String.valueOf(same_flag));
//                    context.write(keyout, valueout);
//                    keyout.set("inputkeys");
//                    valueout.set(inputkeys.toString());
//                    context.write(keyout, valueout);
//                    keyout.set("inputvalues");
//                    valueout.set(inputvalues.toString());
//                    context.write(keyout, valueout);
//                    keyout.set("key");
//                    valueout.set(key.toString());
//                    context.write(keyout, valueout);
//                    keyout.set("value");
//                    valueout.set(value.toString());
//                    context.write(keyout, valueout);
                    if (same_flag) {
                        ArrayList<Integer> tmp_value = new ArrayList<>();
                        ArrayList<String> tmp_key = new ArrayList<>();
                        for (int l = 1; l <= lim; l++) {
                            int index = -1;
                            if ((index = value.indexOf(l)) > -1) {
                                tmp_key.add(key.get(index));
                                tmp_value.add(value.get(index));
                                continue;
                            } else if ((index = inputvalues.indexOf(l)) > -1) {
                                tmp_key.add(inputkeys.get(index));
                                tmp_value.add(inputvalues.get(index));
                                continue;
                            } else {
                                continue;
                            }
                        }
                        if(tmp_value.size() == lim){
                            String outKey = new String();
                            String outValue = new String();
                            for(String key_2: tmp_key){
                                outKey += key_2 + " ";
                            }
                            for(Integer value_2: tmp_value){
                                outValue += value_2.toString() + " ";
                            }
                            outKey = outKey.trim();
                            outValue = outValue.trim();
                            Text outKey_t = new Text();
                            Text outValue_t = new Text();
                            outKey_t.set(outKey);
                            outValue_t.set(outValue);
                            context.write(outKey_t, outValue_t);
                        }else{
                            boolean cont_flag = false;
                            for(ArrayList<String> key_1: keys){
                                if(tmp_key.equals(key_1)){
                                    cont_flag = !cont_flag;
                                    break;
                                }
                            }
                            if(!cont_flag){
                                keys.add(tmp_key);
                                keys.add(inputkeys);
                                values.add(tmp_value);
                                values.add(inputvalues);
                            }
                        }

                    } else {
                        boolean cont_flag = false;
                        for(ArrayList<String> key_1: keys){
                            if(inputkeys.equals(key_1)){
                                cont_flag = !cont_flag;
                                break;
                            }
                        }
                        if(!cont_flag){
                            keys.add(inputkeys);
                            values.add(inputvalues);
                        }
                    }
                }
            }
        }
//        Text keyout = new Text();
//        Text valueout = new Text();
//        for(int i = 0; i < values.size(); i++){
//            keyout.set("!!!!!key!!!!!");
//            valueout.set(keys.get(i).toString());
//            context.write(keyout, valueout);
//            keyout.set("!!!!!value!!!!!");
//            valueout.set(values.get(i).toString());
//            context.write(keyout, valueout);
//        }
    }
}