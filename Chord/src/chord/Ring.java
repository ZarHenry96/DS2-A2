package chord;

/**
 * This class represents the Chord ring
 */
public class Ring {
	private float radius;
	
	/**
	 * Public constructor
	 * @param radius the radius of the Chord ring
	 */
	public Ring(float radius) {
		this.radius = radius;
	}
	
	/**
	 * Returns the radius of the Chord ring
	 * @return the radius of the Chord ring
	 */
	public float getRadius() {
		return this.radius;
	}
}
