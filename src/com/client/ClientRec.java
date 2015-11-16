package com.client;

import java.io.IOException;

import com.client.ClientFS.FSReturnVals;
import com.master.Master;

public class ClientRec {

	/**
	 * Appends a record to the open file as specified by ofh Returns BadHandle
	 * if ofh is invalid Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
	 */
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		
		FSReturnVals converted = null;
		
		try {
			// send command
			ClientFS.WriteOutput.writeInt(Master.AppendRecordCMD);
			
			// send arguments
			ClientFS.WriteOutput.writeObject(ofh);
			ClientFS.WriteOutput.flush();
			ClientFS.WriteOutput.writeObject(RecordID);
			ClientFS.WriteOutput.flush();
			ClientFS.WriteOutput.writeInt(payload.length);
			ClientFS.WriteOutput.flush();

			// retrieve response
			FileHandle tempFH = (FileHandle) ClientFS.ReadInput.readObject();
			ofh.ChunkServerStatus = tempFH.ChunkServerStatus;
			ofh.FilePath = tempFH.FilePath;
			
			RID tempRID = (RID) ClientFS.ReadInput.readObject();
			RecordID.chunkhandle = tempRID.chunkhandle;
			RecordID.byteoffset = tempRID.byteoffset;
			RecordID.size = tempRID.size;
			
			String retval = ClientFS.ReadInput.readUTF();
			converted = FSReturnVals.valueOf(retval);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		// request info from master
		if (converted != FSReturnVals.Success) {
			return converted;
		}
		
		
//		FSReturnVals retval = ClientFS.master.AppendRecord(ofh, RecordID, payload.length);
//		if (retval != FSReturnVals.Success) {
//			return retval;
//		}
		
		// write to chunkserver
		ClientFS.chunkserver1.writeChunk(RecordID.chunkhandle, payload, RecordID.byteoffset);
//		ClientFS.client.writeChunk(RecordID.chunkhandle, payload, RecordID.byteoffset);
		
		return FSReturnVals.Success;
	}

	/**
	 * Deletes the specified record by RecordID from the open file specified by
	 * ofh Returns BadHandle if ofh is invalid Returns BadRecID if the specified
	 * RID is not valid Returns RecDoesNotExist if the record specified by
	 * RecordID does not exist.
	 *
	 * Example usage: DeleteRecord(FH1, RecID1)
	 */
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {
		
		FSReturnVals converted = null;
		
		try {
			// send command
			ClientFS.WriteOutput.writeInt(Master.DeleteRecordCMD);
			
			// send arguments
			ClientFS.WriteOutput.writeObject(ofh);
			ClientFS.WriteOutput.flush();
			ClientFS.WriteOutput.writeObject(RecordID);
			ClientFS.WriteOutput.flush();

			// retrieve response
			FileHandle tempFH = (FileHandle) ClientFS.ReadInput.readObject();
			ofh.ChunkServerStatus = tempFH.ChunkServerStatus;
			ofh.FilePath = tempFH.FilePath;
			
			RID tempRID = (RID) ClientFS.ReadInput.readObject();
			RecordID.chunkhandle = tempRID.chunkhandle;
			RecordID.byteoffset = tempRID.byteoffset;
			RecordID.size = tempRID.size;
			
			String retval = ClientFS.ReadInput.readUTF();
			converted = FSReturnVals.valueOf(retval);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		// request info from master
		if (converted != FSReturnVals.Success) {
			return converted;
		}
		
		return FSReturnVals.Success;
		
//		return ClientFS.master.DeleteRecord(ofh, RecordID);
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, RecID1)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec) {
		
		FSReturnVals converted = null;
		
		try {
			// send command
			ClientFS.WriteOutput.writeInt(Master.ReadFirstRecordCMD);
			
			// send arguments
			ClientFS.WriteOutput.writeObject(ofh);
			ClientFS.WriteOutput.flush();

			// retrieve response
			FileHandle tempFH = (FileHandle) ClientFS.ReadInput.readObject();
			ofh.ChunkServerStatus = tempFH.ChunkServerStatus;
			ofh.FilePath = tempFH.FilePath;
			
			RID tempRID = (RID) ClientFS.ReadInput.readObject();
			rec.setRID(tempRID);
			
			String retval = ClientFS.ReadInput.readUTF();
			converted = FSReturnVals.valueOf(retval);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		// request info from master
		if (converted != FSReturnVals.Success) {
			rec.setRID(null);
			return converted;
		}
		
		// request info from master
//		RID tempRID = new RID();
//		FSReturnVals retval = ClientFS.master.ReadFirstRecord(ofh, tempRID);
//		rec.setRID(tempRID);
//		if (retval != FSReturnVals.Success) {
//			return retval;
//		}
		
//		// read from ChunkServer
		byte[] tempPayload = ClientFS.chunkserver1.readChunk(rec.getRID().chunkhandle, rec.getRID().byteoffset, rec.getRID().size);
//		byte[] tempPayload = ClientFS.client.readChunk(rec.getRID().chunkhandle, rec.getRID().byteoffset, rec.getRID().size);
		rec.setPayload(tempPayload);
		
		return FSReturnVals.Success;
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, RecID1)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec) {
		
		FSReturnVals converted = null;
		
		try {
			// send command
			ClientFS.WriteOutput.writeInt(Master.ReadLastRecordCMD);
			
			// send arguments
			ClientFS.WriteOutput.writeObject(ofh);
			ClientFS.WriteOutput.flush();

			// retrieve response
			FileHandle tempFH = (FileHandle) ClientFS.ReadInput.readObject();
			ofh.ChunkServerStatus = tempFH.ChunkServerStatus;
			ofh.FilePath = tempFH.FilePath;
			
			RID tempRID = (RID) ClientFS.ReadInput.readObject();
			rec.setRID(tempRID);
			
			String retval = ClientFS.ReadInput.readUTF();
			converted = FSReturnVals.valueOf(retval);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		// request info from master
		if (converted != FSReturnVals.Success) {
			rec.setRID(null);
			return converted;
		}
		
		// request info from master
//		RID tempRID = new RID();
//		FSReturnVals retval = ClientFS.master.ReadLastRecord(ofh, tempRID);
//		rec.setRID(tempRID);
//		if (retval != FSReturnVals.Success) {
//			return retval;
//		}
		
		// read from ChunkServer
		byte[] tempPayload = ClientFS.chunkserver1.readChunk(rec.getRID().chunkhandle, rec.getRID().byteoffset, rec.getRID().size);
//		byte[] tempPayload = ClientFS.client.readChunk(rec.getRID().chunkhandle, rec.getRID().byteoffset, rec.getRID().size);
		rec.setPayload(tempPayload);
		
		return FSReturnVals.Success;
	}

	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, tinyRec1) 2. ReadNextRecord(FH1,
	 * rec1, tinyRec2) 3. ReadNextRecord(FH1, rec2, tinyRec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec) {
		
		FSReturnVals converted = null;
		
		try {
			// send command
			ClientFS.WriteOutput.writeInt(Master.ReadNextRecordCMD);
			
			// send arguments
			ClientFS.WriteOutput.writeObject(ofh);
			ClientFS.WriteOutput.flush();
			ClientFS.WriteOutput.writeObject(pivot);
			ClientFS.WriteOutput.flush();

			// retrieve response
			FileHandle tempFH = (FileHandle) ClientFS.ReadInput.readObject();
			ofh.ChunkServerStatus = tempFH.ChunkServerStatus;
			ofh.FilePath = tempFH.FilePath;
			
			RID tempRID = (RID) ClientFS.ReadInput.readObject();
			rec.setRID(tempRID);
			
			String retval = ClientFS.ReadInput.readUTF();
			converted = FSReturnVals.valueOf(retval);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		// request info from master
		if (converted != FSReturnVals.Success) {
			rec.setRID(null);
			return converted;
		}
		
		// request info from master
//		RID tempRID = new RID();
//		FSReturnVals retval = ClientFS.master.ReadNextRecord(ofh, pivot, tempRID);
//		rec.setRID(tempRID);
//		
//		if (retval != FSReturnVals.Success) {
//			return retval;
//		}
		
		// read from ChunkServer
		byte[] tempPayload = ClientFS.chunkserver1.readChunk(rec.getRID().chunkhandle, rec.getRID().byteoffset, rec.getRID().size);
//		byte[] tempPayload = ClientFS.client.readChunk(rec.getRID().chunkhandle, rec.getRID().byteoffset, rec.getRID().size);
		rec.setPayload(tempPayload);
		
		return FSReturnVals.Success;
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, tinyRec1) 2. ReadPrevRecord(FH1,
	 * recn-1, tinyRec2) 3. ReadPrevRecord(FH1, recn-2, tinyRec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec) {
		
		FSReturnVals converted = null;
		
		try {
			// send command
			ClientFS.WriteOutput.writeInt(Master.ReadPrevRecordCMD);
			
			// send arguments
			ClientFS.WriteOutput.writeObject(ofh);
			ClientFS.WriteOutput.flush();
			ClientFS.WriteOutput.writeObject(pivot);
			ClientFS.WriteOutput.flush();

			// retrieve response
			FileHandle tempFH = (FileHandle) ClientFS.ReadInput.readObject();
			ofh.ChunkServerStatus = tempFH.ChunkServerStatus;
			ofh.FilePath = tempFH.FilePath;
			
			RID tempRID = (RID) ClientFS.ReadInput.readObject();
			rec.setRID(tempRID);
			
			String retval = ClientFS.ReadInput.readUTF();
			converted = FSReturnVals.valueOf(retval);
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		
		
		// request info from master
		if (converted != FSReturnVals.Success) {
			rec.setRID(null);
			return converted;
		}
		
		// request info from master
//		RID tempRID = new RID();
//		FSReturnVals retval = ClientFS.master.ReadPrevRecord(ofh, pivot, tempRID);
//		rec.setRID(tempRID);
//		if (retval != FSReturnVals.Success) {
//			return retval;
//		}
		
		// read from ChunkServer
		byte[] tempPayload = ClientFS.chunkserver1.readChunk(rec.getRID().chunkhandle, rec.getRID().byteoffset, rec.getRID().size);
//		byte[] tempPayload = ClientFS.client.readChunk(rec.getRID().chunkhandle, rec.getRID().byteoffset, rec.getRID().size);
		rec.setPayload(tempPayload);
		
		return FSReturnVals.Success;
	}

}
