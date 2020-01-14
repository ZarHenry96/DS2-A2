package chord;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/**
 * This class provides some utility methods
 */
public class Utils {
	
	/**
	 * Returns the hash value of the given key w.r.t. the specified hash size (SHA-1 is used)
	 * @param key the key of interest
	 * @param hashSize number of bits of the hash value
	 * @return the hash value of the key
	 */
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
	 * @param rnd random number generator
	 * @param lambda of the exponential distribution
	 * @param maximum maximum allowed value
	 * @return next packet delay
	 */
	public static double getNextDelay(Random rnd, double lambda, double maximum) {
		double delay = Math.log(1-rnd.nextDouble())/(-lambda);
		return delay < maximum ? delay : maximum;
	}
	
	/**
	 * Verifies if the value belongs to the interval defined by the specified end-points in modular arithmetics
	 * @param value the value to check
	 * @param lower_bound lower end-point of the interval (excluded)
	 * @param upper_bound upper end-point of the interval (included)
	 * @return true if the value belongs to the interval, false otherwise
	 */
	public static boolean belongsToInterval(int value, int lower_bound, int upper_bound) {
		if(lower_bound < upper_bound) {
			return (value > lower_bound && value <= upper_bound);
		} else {
			return (value > lower_bound || value <= upper_bound);
		}
	}
}
