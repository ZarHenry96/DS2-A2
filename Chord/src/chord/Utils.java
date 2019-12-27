package chord;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {
	private static Integer maxId = null;
	
	public static void setMaxId(Integer hashSize) {
		if (Utils.maxId == null) {
			Utils.maxId = Double.valueOf(Math.pow(2,hashSize)).intValue();
		}
	}
	
	public static Integer getHash(String key)
	{
	    BigInteger sha1 = null;
	    try {
	        MessageDigest crypt = MessageDigest.getInstance("SHA-1");
	        crypt.reset();
	        crypt.update(key.getBytes("UTF-8"));
	        sha1 = new BigInteger(1,crypt.digest()).mod(new BigInteger(Utils.maxId.toString()));
	    } catch(NoSuchAlgorithmException e) {
	        e.printStackTrace();
	    } catch(UnsupportedEncodingException e) {
	        e.printStackTrace();
	    }
	    return sha1.intValue();
	}
}
