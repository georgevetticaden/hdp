package hortonworks.hdp.refapp.ecm.util;

import java.util.Random;

import org.apache.commons.codec.binary.Hex;

public final class DocumentGUIDUtils {
	
	public static final String generateDocGUID(){
		return getRandomString(15);
	}
	
	private static String getRandomString(int stringLength) {
		Random random = new Random();
		byte[] bytes = new byte[30];
		random.nextBytes(bytes);
		String key = Hex.encodeHexString(bytes);
		return key.substring(0,stringLength);
	}		

}
