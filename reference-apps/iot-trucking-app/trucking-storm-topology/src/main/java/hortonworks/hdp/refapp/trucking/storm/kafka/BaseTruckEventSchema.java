package hortonworks.hdp.refapp.trucking.storm.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseTruckEventSchema implements Scheme {
	
	private static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;	
	
	private static final Logger LOG = LoggerFactory.getLogger(BaseTruckEventSchema.class);

	public static String[] deserializeRawString(ByteBuffer string) {
        String deserializedRawString = null;
		if (string.hasArray()) {
            int base = string.arrayOffset();
            deserializedRawString = new String(string.array(), base + string.position(), string.remaining());
        } else {
        	deserializedRawString = new String(Utils.toByteArray(string), UTF8_CHARSET);
        }
		LOG.debug("Deserialized raw event is: " + deserializedRawString);
		String[] pieces = deserializedRawString.split("\\|");
		LOG.debug("Tokenized raw event is: " + pieces);
		return pieces;
    }	


}
