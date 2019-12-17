package chord;

import java.util.Random;

import repast.simphony.engine.schedule.ScheduledMethod;
import repast.simphony.space.graph.Network;

public class Node {
	private Network<Object> viewNet;
	private Random rnd;
	private int hash_size;

	private Integer id;
	private double x;
	private double y;
	private boolean subscribed = true;
	private boolean crashed = false;
	private double crash_pr;
	private double recovery_interval;
	private double message_loss;
	
	public Node(Network<Object> viewNet, Random rnd, int hash_size, int id, double x, double y, double crash_pr, double recovery_interval, double message_loss) {
		this.viewNet = viewNet;
		this.rnd = rnd;
		this.hash_size = hash_size;
		this.id = id;
		this.x = x;
		this.y = y;
		this.crash_pr = crash_pr;
		this.recovery_interval = recovery_interval;
		this.message_loss = message_loss;
	}
	
	@ScheduledMethod(start = 1, interval = 1)
	public void method() {
	
	}
	
	public int getHashSize() {
		return this.hash_size;
	}
	
	public int getId() {
		return this.id;
	}
	
	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}
}
