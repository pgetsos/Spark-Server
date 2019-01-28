/*
 * Copyright (c) 2019.
 * Created for MSc in Computer Science - Distributed Systems
 * All right reserved except otherwise noted
 */

package auebdreamteam.com.dssparkclient.entities;

import auebdreamteam.com.dssparkclient.entities.BaseQueryClass;

public class MapQuery extends BaseQueryClass {

	private double startingLongCoordinate;
	private double startingLatCoordinate;
	private double endingLongCoordinate;
	private double endingLatCoordinate;

	public double getStartingLongCoordinate() {
		return startingLongCoordinate;
	}

	public void setStartingLongCoordinate(double startingLongCoordinate) {
		this.startingLongCoordinate = startingLongCoordinate;
	}

	public double getStartingLatCoordinate() {
		return startingLatCoordinate;
	}

	public void setStartingLatCoordinate(double startingLatCoordinate) {
		this.startingLatCoordinate = startingLatCoordinate;
	}

	public double getEndingLongCoordinate() {
		return endingLongCoordinate;
	}

	public void setEndingLongCoordinate(double endingLongCoordinate) {
		this.endingLongCoordinate = endingLongCoordinate;
	}

	public double getEndingLatCoordinate() {
		return endingLatCoordinate;
	}

	public void setEndingLatCoordinate(double endingLatCoordinate) {
		this.endingLatCoordinate = endingLatCoordinate;
	}
}
