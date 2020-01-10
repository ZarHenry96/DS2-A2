package chord;

import java.awt.Color;
import java.awt.Font;

import repast.simphony.visualizationOGL2D.DefaultStyleOGL2D;
import saf.v3d.scene.Position;
import saf.v3d.scene.VSpatial;

public class NodeStyle extends DefaultStyleOGL2D {
	
	public VSpatial getVSpatial(Object object, VSpatial spatial) {
		if (spatial == null) {
	    	if(object instanceof Node) {
	    		Node n = (Node)object;
	    		spatial = shapeFactory.createCircle(80 * (1f/n.getHashSize()), 16);
	    	}
	    }
	    return spatial;
	  }
	
	@Override
	public Color getColor(Object object) {
		if(object instanceof Node) {
			Node n = (Node)object;
			if(n.isSubscribed()) {
				if(n.isInitialized()) {
					if(n.isCrashed()) {
						return Color.RED;
					} else {
						return Color.GREEN;
					}
				} else {
					return Color.ORANGE;
				}
			} else {
				return new Color(0, 0, 0, 1);
			}
		}
		return null;
	}
	
	@Override
	public String getLabel(Object object) {
		if(object instanceof Node) {
			Node n = (Node)object;
			if(n.isSubscribed()) {
				String label = "Id: "+ (n.getId() < 10 || n.getId() > 99 ? String.valueOf(n.getId())+"  " : String.valueOf(n.getId()));
				label += "\n\nFinger:";
				label += n.getFinger().toString();
				label += "\nSucc: "+(n.getSuccessors().isEmpty() ? "-" : n.getSuccessors().get(0).getId());
				label += "\nPred: "+(n.getPredecessor() == null ? "-" : n.getPredecessor().getId());
				return label;
			} else {
				return " ";
			}
		}
		return null;
	}

	@Override
	public Font getLabelFont(Object object) {
		if(object instanceof Node) {
			Node n = (Node)object;
			return new Font("Calibri", Font.PLAIN, (int)(120*(1f/n.getHashSize())));
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
	    	int sign = (node.getId() % 2 == 0) ? - 1 : +1;
	    	return sign*62f*String.valueOf(num_nodes).length()*Float.valueOf(String.valueOf(Math.sin(Math.toRadians((360.0/num_nodes)*node.getId()))));
	    }
		return 0;
	}
	
	@Override
	public float getLabelYOffset(Object object) {
		if(object instanceof Node) {
	    	Node node = (Node) object;
	    	int num_nodes = Double.valueOf(Math.pow(2, node.getHashSize())).intValue();
	    	int sign = (node.getId() % 2 == 0) ? + 1 : -1;
	    	return sign*62f*String.valueOf(num_nodes).length()*Float.valueOf(String.valueOf(Math.cos(Math.toRadians((360.0/num_nodes)*node.getId()))));
	    }
		return 0;
	}
}
