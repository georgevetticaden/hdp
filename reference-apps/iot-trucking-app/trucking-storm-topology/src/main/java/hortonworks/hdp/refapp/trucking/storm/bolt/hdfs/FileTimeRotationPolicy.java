package hortonworks.hdp.refapp.trucking.storm.bolt.hdfs;


import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.tuple.Tuple;

/**
 * File rotation policy that will rotate files after a certain
 * amount of time has pass
 *
 * For example:
 * <pre>
 *     // rotate files every 30 minutes
 *     FileSizeRotationPolicy policy =
 *          new FileTimeRotationPolicy(30.0, Units.MINUTES);
 * </pre>
 *
 */
public class FileTimeRotationPolicy implements FileRotationPolicy {
    /**
	 * 
	 */
	private static final long serialVersionUID = 2511856964496167738L;
	
	private static final Logger LOG = LoggerFactory.getLogger(FileTimeRotationPolicy.class);

    public static enum Units {

        SECONDS((long)1000),
        MINUTES((long)1000*60),
        HOURS((long)1000*60*60),
        DAYS((long)1000*60*60);

        private long milliSeconds;

        private Units(long milliSeconds){
            this.milliSeconds = milliSeconds;
        }

        public long getMilliSeconds(){
            return milliSeconds;
        }
    }

    private long maxMilliSeconds;
    private long lastCheckpoint = new Long((new Date()).getTime());


    public FileTimeRotationPolicy(float count, Units units){
        this.maxMilliSeconds = (long)(count * units.getMilliSeconds());
    }

    @Override
    public boolean mark(Tuple tuple, long offset) {
        // The offsett is not used here as we are rotating based on time
        long diff = (new Date()).getTime() - this.lastCheckpoint;
        return diff >= this.maxMilliSeconds;
    }

    @Override
    public void reset() {
        this.lastCheckpoint =  new Long((new Date()).getTime());
    }

}