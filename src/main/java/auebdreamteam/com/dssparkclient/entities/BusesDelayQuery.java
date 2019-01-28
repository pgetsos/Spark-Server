/*
 * Copyright (c) 2019.
 * Created for MSc in Computer Science - Distributed Systems
 * All right reserved except otherwise noted
 */

package auebdreamteam.com.dssparkclient.entities;

import auebdreamteam.com.dssparkclient.entities.BaseQueryClass;

public class BusesDelayQuery extends BaseQueryClass {

	private String date;
	private String lineID;
	private int stopID;

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getLineID() {
		return lineID;
	}

	public void setLineID(String lineID) {
		this.lineID = lineID;
	}

	public int getStopID() {
		return stopID;
	}

	public void setStopID(int stopID) {
		this.stopID = stopID;
	}
}
