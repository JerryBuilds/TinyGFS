package com.client;

public class RID {
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
