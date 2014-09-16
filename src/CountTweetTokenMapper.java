import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountTweetTokenMapper
	extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	@Override
    /**
      * map method that performs the tokenizer job and framing the initial key value pairs
      * @param  key  is a long integer offset.
      * @param  value is a line of text.
      * @param  context is an instance of Context to write output to.
      **/
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		//taking one line at a time
		String line = value.toString();
		
		Configuration conf = context.getConfiguration();
		String tokens = conf.get("tokens");

		String[] tokens_arr = tokens.split(",");
		String token = "";
		
		for (int index=0; index< tokens_arr.length; index++)
		{
			// Remove unwanted heading and trailing spaces and convert into lowercase.
			token = tokens_arr[index].trim().toLowerCase();
			context.write(new Text(token), 
						  line.toLowerCase().contains(token) ? new LongWritable(1) : new LongWritable(0));
		}
	}
}
