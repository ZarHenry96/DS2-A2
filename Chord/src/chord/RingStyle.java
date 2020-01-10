package chord;

import java.awt.Color;

import repast.simphony.visualizationOGL2D.DefaultStyleOGL2D;
import saf.v3d.scene.VSpatial;

public class RingStyle extends DefaultStyleOGL2D {
	
	@Override
	public VSpatial getVSpatial(Object agent, VSpatial spatial) {
		if (spatial == null) {
			if(agent instanceof Ring) {
				spatial = shapeFactory.createCircle(((Ring) agent).getRadius(), 720);
			}
	    }
	    return spatial;
	}
	
	@Override
	public Color getColor(Object agent) {
	    return Color.WHITE;
	}
	
	@Override
	public int getBorderSize(Object object) {
	    return 3;
	}
	
	@Override
	public Color getBorderColor(Object object) {
	    return Color.BLUE;
	}
	
	@Override
	public float getScale(Object object) {
	    return 15f;
	}
}
