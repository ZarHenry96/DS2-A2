package chord;

import java.awt.Font;

import repast.simphony.visualizationOGL2D.DefaultStyleOGL2D;
import saf.v3d.scene.Position;

public class NodeStyle extends DefaultStyleOGL2D {

	@Override
	public String getLabel(Object object) {
		if(object instanceof Node) {
			Node n = (Node)object;
			return n.getId() < 10 || n.getId() > 99 ? String.valueOf(n.getId())+"  " : String.valueOf(n.getId()); 
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
	
	@Override
	public Position getLabelPosition(Object object) {
	    return Position.CENTER;
	}
	
	@Override
	public float getLabelXOffset(Object object) {
	    if(object instanceof Node) {
	    	Node node = (Node) object;
	    	int num_nodes = Double.valueOf(Math.pow(2, node.getHashSize())).intValue();
	    	return -15.5f*String.valueOf(num_nodes).length()*Float.valueOf(String.valueOf(Math.sin(Math.toRadians((360.0/num_nodes)*node.getId()))));
	    }
		return 0;
	}
	
	@Override
	public float getLabelYOffset(Object object) {
		if(object instanceof Node) {
	    	Node node = (Node) object;
	    	int num_nodes = Double.valueOf(Math.pow(2, node.getHashSize())).intValue();
	    	return -15.5f*String.valueOf(num_nodes).length()*Float.valueOf(String.valueOf(Math.cos(Math.toRadians((360.0/num_nodes)*node.getId()))));
	    }
		return 0;
	}
}
