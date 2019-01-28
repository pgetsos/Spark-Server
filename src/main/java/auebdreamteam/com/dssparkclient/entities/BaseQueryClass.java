/*
 * Copyright (c) 2019.
 * Created for MSc in Computer Science - Distributed Systems
 * All right reserved except otherwise noted
 */

package auebdreamteam.com.dssparkclient.entities;

import java.io.Serializable;

public abstract class BaseQueryClass implements Serializable {

	private static final long serialVersionUID = 12345678904L;

	private String serverIP;
	public String getServerIP() {
		return serverIP;
	}

	public void setServerIP(String serverIP) {
		this.serverIP = serverIP;
	}
}
