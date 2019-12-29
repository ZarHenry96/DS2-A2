package chord;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class Utils {
	
	public static Integer getHash(String key, int hashSize) {
		Integer maxValue = Double.valueOf(Math.pow(2,hashSize)).intValue();
	    BigInteger sha1 = null;
	    try {
	        MessageDigest crypt = MessageDigest.getInstance("SHA-1");
	        crypt.reset();
	        crypt.update(key.getBytes("UTF-8"));
	        sha1 = new BigInteger(1,crypt.digest()).mod(new BigInteger(maxValue.toString()));
	    } catch(NoSuchAlgorithmException e) {
	        e.printStackTrace();
	    } catch(UnsupportedEncodingException e) {
	        e.printStackTrace();
	    }
	    return sha1.intValue();
	}
	
	/**
	 * Returns the next packet delay, sampled from an exponential distribution with lambda = mean_packet_delay
	 * @return next packet delay
	 */
	public static double getNextDelay(Random rnd, double lambda, double maximum) {
		double delay = Math.log(1-rnd.nextDouble())/(-lambda);
		return delay < maximum ? delay : maximum;
	}
	
	public static boolean belongsToInterval(int value, int lower_bound, int upper_bound) {
		if(lower_bound < upper_bound) {
			return (value > lower_bound && value <= upper_bound);
		} else {
			return (value > lower_bound || value <= upper_bound);
		}
	}
}
