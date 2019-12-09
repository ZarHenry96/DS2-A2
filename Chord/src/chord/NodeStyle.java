package chord;

import java.awt.Font;

import repast.simphony.visualizationOGL2D.DefaultStyleOGL2D;

public class NodeStyle extends DefaultStyleOGL2D {

	@Override
	public String getLabel(Object object) {
		if(object instanceof Node) {
			Node n = (Node)object;
			return String.valueOf(n.getId());
		}
		return null;
	}

	@Override
	public Font getLabelFont(Object object) {
		if(object instanceof Node) {
			Node n = (Node)object;
			return new Font("Calibri", Font.PLAIN, n.getHashSize()*10);
		}
		return null;
	}
	
}
