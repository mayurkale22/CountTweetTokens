import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountTweetTokenReducer 
	extends Reducer<Text, LongWritable, Text, LongWritable>{
	@Override
	/**
	 *  reduce method accepts the Key Value pairs from mappers, 
	 *  do the aggregation based on keys and produce the final out put.
	 *  @param  key, values, context
	 **/
	public void reduce(Text key, Iterable<LongWritable> values, Context context) 
		throws IOException, InterruptedException{
		
		long tokenCount = 0L;
		
        /** 
         * iterates through all the values available with a key and add them together and give the
         * final result as the key and sum of its values
         **/
		for (LongWritable value : values) {
			tokenCount += value.get();
		}
		
		context.write(key, new LongWritable(tokenCount));
	}
}