package com.client;

import java.io.Serializable;

public class RID implements Serializable {
	public String chunkhandle;
	public int byteoffset;
	public int size;
//	public int index;
	
	public boolean equals(RID rhs) {
		if (this.chunkhandle.equals(rhs.chunkhandle)
				&& this.byteoffset == rhs.byteoffset
				&& this.size == rhs.size) {
			return true;
		}
		return false;
	}
}
