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
	 * Example usage: ReadFirstRecord(FH1, rec, recid)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		
		FSReturnVals converted = null;
		
		try {
			// send command
			ClientFS.WriteOutput.writeInt(Master.ReadFirstRecordCMD);
			
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
		
		// request info from master
//		FSReturnVals retval = ClientFS.master.ReadFirstRecord(ofh, RecordID);
//		if (retval != FSReturnVals.Success) {
//			return retval;
//		}
		
		// read from ChunkServer
		byte[] temp = ClientFS.chunkserver1.readChunk(RecordID.chunkhandle, RecordID.byteoffset, RecordID.size);
		for (int i=0; i < RecordID.size; i++) {
			payload[i] = temp[i];
		}
		
		return FSReturnVals.Success;
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, rec, recid)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		
		FSReturnVals converted = null;
		
		try {
			// send command
			ClientFS.WriteOutput.writeInt(Master.ReadLastRecordCMD);
			
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
		
		// request info from master
//		FSReturnVals retval = ClientFS.master.ReadLastRecord(ofh, RecordID);
//		if (retval != FSReturnVals.Success) {
//			return retval;
//		}
		
		// read from ChunkServer
		byte[] temp = new byte[RecordID.size];
		temp = ClientFS.chunkserver1.readChunk(RecordID.chunkhandle, RecordID.byteoffset, RecordID.size);
		for (int i=0; i < RecordID.size; i++) {
			payload[i] = temp[i];
		}
		
		return FSReturnVals.Success;
	}

	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, rec, rec1) 2. ReadNextRecord(FH1,
	 * rec1, rec, rec2) 3. ReadNextRecord(FH1, rec2, rec, rec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, byte[] payload, RID RecordID) {
		
		FSReturnVals converted = null;
		
		try {
			// send command
			ClientFS.WriteOutput.writeInt(Master.ReadNextRecordCMD);
			
			// send arguments
			ClientFS.WriteOutput.writeObject(ofh);
			ClientFS.WriteOutput.flush();
			ClientFS.WriteOutput.writeObject(pivot);
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
		
		// request info from master
//		FSReturnVals retval = ClientFS.master.ReadNextRecord(ofh, pivot, RecordID);
//		if (retval != FSReturnVals.Success) {
//			return retval;
//		}
		
		// read from ChunkServer
		byte[] temp = new byte[RecordID.size];
		temp = ClientFS.chunkserver1.readChunk(RecordID.chunkhandle, RecordID.byteoffset, RecordID.size);
		for (int i=0; i < RecordID.size; i++) {
			payload[i] = temp[i];
		}
		
		return FSReturnVals.Success;
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, rec, recn) 2. ReadPrevRecord(FH1,
	 * recn-1, rec, rec2) 3. ReadPrevRecord(FH1, recn-2, rec, rec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, byte[] payload, RID RecordID) {
		
		FSReturnVals converted = null;
		
		try {
			// send command
			ClientFS.WriteOutput.writeInt(Master.ReadPrevRecordCMD);
			
			// send arguments
			ClientFS.WriteOutput.writeObject(ofh);
			ClientFS.WriteOutput.flush();
			ClientFS.WriteOutput.writeObject(pivot);
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
		
		// request info from master
//		FSReturnVals retval = ClientFS.master.ReadPrevRecord(ofh, pivot, RecordID);
//		if (retval != FSReturnVals.Success) {
//			return retval;
//		}
		
		// read from ChunkServer
		byte[] temp = new byte[RecordID.size];
		temp = ClientFS.chunkserver1.readChunk(RecordID.chunkhandle, RecordID.byteoffset, RecordID.size);
		for (int i=0; i < RecordID.size; i++) {
			payload[i] = temp[i];
		}
		
		return FSReturnVals.Success;
	}

}
