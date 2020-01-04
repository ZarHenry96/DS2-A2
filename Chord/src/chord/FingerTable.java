package chord;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class FingerTable {
	private int size;
	private ConcurrentHashMap<Integer, Node> table;
	
	public FingerTable(int size) {
		this.size = size;
		this.table = new ConcurrentHashMap<>();
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
		CopyOnWriteArrayList<Integer> indices = this.getKeys(false);
		for(int index: indices) {
			if(this.table.get(index).equals(dead)) {
				this.table.remove(index);
			}
		}
	}
	
	public CopyOnWriteArrayList<Integer> getKeys(boolean descending_order) {
		CopyOnWriteArrayList<Integer> keys = new CopyOnWriteArrayList<>(this.table.keySet());
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
	
	@Override
	public String toString() {
		String out = "";
		
		CopyOnWriteArrayList<Integer> keys = this.getKeys(false);
		for(int key: keys) {
			out += "\n\t"+key+"\t"+this.table.get(key).getId();
		}
		
		return out;
	}
}
