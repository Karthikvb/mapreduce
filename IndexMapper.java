package linecount;

import java.io.IOException;
import java.util.StringTokenizer;
// import org.apache.commons.logging.Log;
// import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


@SuppressWarnings("deprecation")
public class IndexMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text> {
    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {
        
    	FileSplit filesplit = (FileSplit) reporter.getInputSplit();    	
    	String fileName = filesplit.getPath().getName();
    	
    	String line = value.toString();
    	
    	StringTokenizer tokenizer = new StringTokenizer(line);
    	while(tokenizer.hasMoreTokens()){
    		String token = tokenizer.nextToken();
    		
    		output.collect(new Text(token.toLowerCase()), new Text(fileName));
    	}
    	
    }
}
