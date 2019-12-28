package chord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class FingerTable {
	private int size;
	private HashMap<Integer, Node> table;
	
	public FingerTable(int size) {
		this.size = size;
		this.table = new HashMap<>();
	}
	
	public Node getEntry(int index) {
		return this.table.get(index);
	}
	
	public void setEntry(int index, Node node) {
		if(index >= 0 && index <= size) {
			this.table.put(index, node);
		}
	}
	
	public void removeEntry(int index) {
		this.table.remove(index);
	}
	
	public void removeEntry(Node dead) {
		for(int index: this.table.keySet()) {
			if(this.table.get(index).equals(dead)) {
				this.table.remove(index);
			}
		}
	}
	
	public ArrayList<Integer> getKeys(boolean descending_order) {
		ArrayList<Integer> keys = new ArrayList<>(this.table.keySet());
		if(descending_order) {
			Collections.sort(keys, Collections.reverseOrder());
		} else {
			Collections.sort(keys);
		}
		
		return keys;
	}
	
	public void clearTable() {
		this.table.clear();
	}
}
