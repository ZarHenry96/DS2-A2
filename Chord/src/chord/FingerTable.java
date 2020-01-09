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
	
	public boolean isEmpty() {
		return this.table.isEmpty();
	}
	
	public Node getEntry(int index) {
		return this.table.get(index);
	}
	
	public int getFirstMissingKey() {
		int i;
		for(i=1; i <= this.size; i++) {
			if(!this.table.keySet().contains(i)){
				return i;
			}
		}
		return i;
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
	
	public void setEntry(int index, Node node) {
		if(index > 0 && index <= size) {
			this.table.put(index, node);
		}
	}
	
	public void removeEntry(int index) {
		this.table.remove(index);
	}
	
	public void removeEntry(Node dead) {
		ArrayList<Integer> indices = this.getKeys(false);
		for(int index: indices) {
			if(this.table.get(index).equals(dead)) {
				this.table.remove(index);
			}
		}
	}
	
	/*
	public void enforceConsistency(int index) {
		Node successor = this.table.get(1);
		Node fixed = this.table.get(index);
		
		for(int i=2; i <= this.size; i++) {
			if(this.table.keySet().contains(i)){
				Node n = this.table.get(i);
				if(i < index && n.getId() != successor.getId() && !Utils.belongsToInterval(n.getId(), successor.getId(), fixed.getId())) {
					this.table.remove(i);
				}
				if(i > index && n.getId() != fixed.getId() && (!Utils.belongsToInterval(n.getId(), fixed.getId(), successor.getId())) || n.getId() == successor.getId()) {
					this.table.remove(i);
				}
			}
		}
	}
	*/
	
	public void clearTable() {
		this.table.clear();
	}
	
	@Override
	public String toString() {
		String out = "";
		
		ArrayList<Integer> keys = this.getKeys(false);
		for(int key: keys) {
			out += "\n\t"+key+"\t"+this.table.get(key).getId();
		}
		
		return out;
	}
}
