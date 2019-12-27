package chord;

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
	
	public void clearTable() {
		this.table.clear();
	}
}
