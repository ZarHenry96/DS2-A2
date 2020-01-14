package chord;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * This class defines the structure of the finger table
 */
public class FingerTable {
	private int size;
	private HashMap<Integer, Node> table;
	
	/**
	 * Public constructor
	 * @param size size of the finger table. Indices in [1,size]
	 */
	public FingerTable(int size) {
		this.size = size;
		this.table = new HashMap<>();
	}
	
	/**
	 * Returns true if the finger table is empty, false otherwise
	 * @return true if the finger table is empty, false otherwise
	 */
	public boolean isEmpty() {
		return this.table.isEmpty();
	}
	
	/**
	 * Returns the Node instance associated to the specified entry
	 * @param index index in the finger table
	 * @return reference to the Node instance pointed by the specified entry
	 */
	public Node getEntry(int index) {
		return this.table.get(index);
	}
	
	/**
	 * Returns the index of the first empty entry
	 * @return the index of the first empty entry (size of the finger table + 1, if none)
	 */
	public int getFirstMissingKey() {
		int i;
		for(i=1; i <= this.size; i++) {
			if(!this.table.keySet().contains(i)){
				return i;
			}
		}
		return i;
	}
	
	/**
	 * Returns the indices of the non-empty entries
	 * @param descending_order true for descending order, false for ascending
	 * @return the indices of the non-empty entries as a list
	 */
	public ArrayList<Integer> getKeys(boolean descending_order) {
		ArrayList<Integer> keys = new ArrayList<>(this.table.keySet());
		if(descending_order) {
			Collections.sort(keys, Collections.reverseOrder());
		} else {
			Collections.sort(keys);
		}
		
		return keys;
	}
	
	/**
	 * Inserts the given node in the specified entry
	 * @param index index in the finger table
	 * @param node reference to the node to insert
	 */
	public void setEntry(int index, Node node) {
		if(index > 0 && index <= size) {
			this.table.put(index, node);
		}
	}
	
	/**
	 * Empties the specified entry
	 * @param index index in the finger table
	 */
	public void removeEntry(int index) {
		this.table.remove(index);
	}
	
	/**
	 * Removes all entries containing the specified node
	 * @param dead reference to the node to remove
	 */
	public void removeEntry(Node dead) {
		ArrayList<Integer> indices = this.getKeys(false);
		for(int index: indices) {
			if(this.table.get(index).equals(dead)) {
				this.table.remove(index);
			}
		}
	}
	
	/**
	 * Empties the table
	 */
	public void clearTable() {
		this.table.clear();
	}
	
	@Override
	public String toString() {
		String out = "";
		
		for(int i=1; i<=this.size; i++) {
			out += "\n"+i+"  "+(this.table.get(i) == null ? "-" : this.table.get(i).getId());
		}
		
		return out;
	}
}
