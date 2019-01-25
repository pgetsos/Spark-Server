/*
 * Copyright (c) 2019.
 * Created for MSc in Computer Science - Distributed Systems
 * All right reserved except otherwise noted
 */

package auebdreamteam.com.dssparkclient.entities;

public class MapPoint {
	private double LongCoordinate;
	private double LatCoordinate;

	public MapPoint(double longCoordinate, double latCoordinate) {
		LongCoordinate = longCoordinate;
		LatCoordinate = latCoordinate;
	}

	public double getLongCoordinate() {
		return LongCoordinate;
	}

	public void setLongCoordinate(double longCoordinate) {
		LongCoordinate = longCoordinate;
	}

	public double getLatCoordinate() {
		return LatCoordinate;
	}

	public void setLatCoordinate(double latCoordinate) {
		LatCoordinate = latCoordinate;
	}
}
