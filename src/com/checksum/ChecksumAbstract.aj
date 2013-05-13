package com.checksum;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.FileProcessor;
import com.sleepycat.je.config.EnvironmentParams;
import com.sleepycat.je.dbi.DbConfigManager;
import com.sleepycat.je.dbi.EnvironmentImpl;
import com.sleepycat.je.log.CleanerFileReader;
import com.sleepycat.je.log.EntryHeader;
import com.sleepycat.je.log.FileManager;
import com.sleepycat.je.log.FileReader;
import com.sleepycat.je.log.INFileReader;
import com.sleepycat.je.log.LastFileReader;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.log.PrintFileReader;
import com.sleepycat.je.log.entry.LogEntry;
import com.sleepycat.je.recovery.RecoveryManager;

public privileged aspect ChecksumAbstract {

	pointcut fileReaderConstructor(FileReader fileReader, EnvironmentImpl env) 
	 : execution(FileReader.new(EnvironmentImpl, int, boolean,	long, Long, long,long)) && 
	 args(env,int, boolean,	long, Long, long,long) && this(fileReader) && within(FileReader);

	pointcut hook_checksumValidation(FileReader fileReader,
			ByteBuffer dataBuffer) 
	 : call(void FileReader.hook_checksumValidation(ByteBuffer)) && target(fileReader) && args(dataBuffer);

	pointcut hook_checkType(FileReader fr, byte currentEntryTypeNum) 
	 : execution(void FileReader.hook_checkType(byte)) && args(currentEntryTypeNum) && this(fr);

	pointcut logManagerConstructor(LogManager logManager, EnvironmentImpl env) 
	: execution(LogManager.new(EnvironmentImpl, boolean)) && this(logManager) && args(env,boolean);

	pointcut addPrevOffset(int entrySize) : execution(ByteBuffer LogManager.addPrevOffset(ByteBuffer,long,int)) && 
		args(ByteBuffer,long,entrySize) && within(LogManager);

	pointcut readHeader(LogManager lm, ByteBuffer entryBuffer,
			EntryHeader entryHeader) 
	 : call(void EntryHeader.readHeader(ByteBuffer, boolean)) && 
		this(lm) && args(entryBuffer,boolean) && target(entryHeader);

	pointcut getRemainingBuffer(LogManager lm, ByteBuffer entryBuffer,
			EntryHeader entryHeader, long lsn) : call(ByteBuffer LogManager.getRemainingBuffer(ByteBuffer, com.sleepycat.je.log.LogSource, long, EntryHeader)) && 
		target(lm) && args(entryBuffer, com.sleepycat.je.log.LogSource, long, entryHeader) && 
		cflow(getLogEntryFromLogSource(lsn));

	pointcut readHeader2(EntryHeader eh, ByteBuffer dataBuffer) 
	: execution(void EntryHeader.readHeader(ByteBuffer,boolean)) 
	&& target(eh) && args(dataBuffer,boolean);

	pointcut lasFileReaderConstructor(FileReader fr) : execution(LastFileReader.new(..)) && 
		this(fr);

	pointcut iNFileReaderConstructor() : (call(INFileReader.new(..)) && withincode(void RecoveryManager.readINsAndTrackIds(long))) ||
		(call(CleanerFileReader.new(..)) && within(FileProcessor));

	pointcut hook_recomputeChecksum(ByteBuffer data, int recStartPos,
			int itemSize) 
	: call(void FileManager.hook_recomputeChecksum(ByteBuffer, int, int)) && 
		args(data,recStartPos,itemSize);

	pointcut readNextEntry() : execution(boolean LastFileReader.readNextEntry());

	pointcut readAndValidateFileHeader(RandomAccessFile newFile,
			String fileName, FileManager fm) 
	 : execution(boolean FileManager.readAndValidateFileHeader(RandomAccessFile, String, long)) &&
		args(newFile,fileName,long) && target(fm) && within(FileManager);

	pointcut readHeader3(ByteBuffer dataBuffer, FileReader fileReader) : execution(void FileReader.readHeader(ByteBuffer)) && 
		args(dataBuffer) && target(fileReader);

	pointcut allocate() : call(ByteBuffer ByteBuffer.allocate(int)) && 
		withincode(ByteBuffer LogManager.marshallIntoBuffer(LoggableObject, int, boolean, int));

	pointcut hook_printChecksum(StringBuffer sb, PrintFileReader pfr) : call(void PrintFileReader.hook_printChecksum(StringBuffer)) && args(sb) && this(pfr);

	pointcut getLogEntryFromLogSource(long lsn) :
		execution(LogEntry LogManager.getLogEntryFromLogSource(long, com.sleepycat.je.log.LogSource)) && args(lsn, com.sleepycat.je.log.LogSource);

	Object around(FileReader fileReader, EnvironmentImpl env)
			throws DatabaseException :
				fileReaderConstructor(fileReader, env)  {
		fileReader.doValidateChecksum = env.getLogManager().getChecksumOnRead();
		Object r = proceed(fileReader, env);

		if (fileReader.doValidateChecksum) {
			fileReader.cksumValidator = new ChecksumValidator();
		}
		fileReader.anticipateChecksumErrors = false;
		return r;
	}

	/**
	 * validate the header and the body
	 */
	after(FileReader fileReader, ByteBuffer dataBuffer)
			throws DatabaseException : 
				hook_checksumValidation(fileReader, dataBuffer){
		boolean doValidate = fileReader.doValidateChecksum
				&& (fileReader.alwaysValidateChecksum || fileReader
						.isTargetEntry(fileReader.currentEntryTypeNum,
								fileReader.currentEntryTypeVersion));
		/* Initialize the checksum with the header. */
		if (doValidate) {
			fileReader.startChecksum(dataBuffer);
		}
		if (doValidate)
			fileReader.currentEntryCollectData = true;
	}

	before(FileReader fr, byte currentEntryTypeNum) throws DatabaseException : 
		hook_checkType(fr, currentEntryTypeNum)	{
		if (!LogEntryType.isValidType(currentEntryTypeNum))
			throw new DbChecksumException((fr.anticipateChecksumErrors ? null
					: fr.env), "FileReader read invalid log entry type: "
					+ currentEntryTypeNum);
	}

	after(LogManager logManager, EnvironmentImpl env) throws DatabaseException : 
		logManagerConstructor(logManager, env) {
		/* See if we're configured to do a checksum when reading in objects. */
		DbConfigManager configManager = env.getConfigManager();
		logManager.doChecksumOnRead = configManager
				.getBoolean(EnvironmentParams.LOG_CHECKSUM_READ);

	}

	ByteBuffer around(int entrySize) : addPrevOffset(entrySize) {

		ByteBuffer destBuffer = proceed(entrySize);

		Checksum checksum = Adler32.makeChecksum();

		/* Now calculate the checksum and write it into the buffer. */
		checksum.update(destBuffer.array(), LogManager.CHECKSUM_BYTES,
				(entrySize - LogManager.CHECKSUM_BYTES));
		LogUtils.writeUnsignedInt(destBuffer, checksum.getValue());
		destBuffer.position(0);
		return destBuffer;
	}

	after(LogManager lm, ByteBuffer entryBuffer, EntryHeader entryHeader)
			throws DatabaseException :
				readHeader(lm, entryBuffer, entryHeader) {
		/* Read the checksum to move the buffer forward. */
		if (lm.doChecksumOnRead) {
			validator = new ChecksumValidator();
			int oldpos = entryBuffer.position();
			entryBuffer.position(oldpos - LogManager.HEADER_CONTENT_BYTES);
			validator.update(lm.envImpl, entryBuffer,
					LogManager.HEADER_CONTENT_BYTES, false);
			entryBuffer.position(oldpos);
		}
	}

	// private pointcut getLogEntryFromLogSource(long lsn) :
	// execution(LogEntry LogManager.getLogEntryFromLogSource(long,LogSource))
	// && args(lsn,LogSource);

	after(LogManager lm, ByteBuffer entryBuffer, EntryHeader entryHeader,
			long lsn) throws DatabaseException :
				getRemainingBuffer(lm, entryBuffer, entryHeader, lsn) {
		/*
		 * Do entry validation. Run checksum before checking the entry type, it
		 * will be the more encompassing error.
		 */
		if (lm.doChecksumOnRead) {
			/* Check the checksum first. */
			validator.update(lm.envImpl, entryBuffer,
					entryHeader.getEntrySize(), false);
			validator.validate(lm.envImpl, entryHeader.getChecksum(), lsn);
		}

	}

	before(EntryHeader eh, ByteBuffer dataBuffer): readHeader2(eh, dataBuffer) {

		/* Get the checksum for this log entry. */
		eh.checksum = LogUtils.getUnsignedInt(dataBuffer);
	}

	after(FileReader fr) : lasFileReaderConstructor(fr) {
		/*
		 * Indicate that a checksum error should not shutdown the whole
		 * environment.
		 */
		fr.anticipateChecksumErrors = true;
	}

	after() returning (FileReader fr) : iNFileReaderConstructor() {

		/*
		 * RecoveryManager: Validate all entries in at least one full recovery
		 * pass.
		 */
		/* FileProcessor: Validate all entries before ever deleting a file. */
		fr.setAlwaysValidateChecksum(true);
	}

	after(ByteBuffer data, int recStartPos, int itemSize) : 
		hook_recomputeChecksum(data,recStartPos,itemSize) {
		Checksum checksum = Adler32.makeChecksum();
		data.position(recStartPos);
		/* Calculate the checksum and write it into the buffer. */
		int nChecksumBytes = itemSize
				+ (LogManager.HEADER_BYTES - LogManager.CHECKSUM_BYTES);
		byte[] checksumBytes = new byte[nChecksumBytes];
		System.arraycopy(data.array(), recStartPos + LogManager.CHECKSUM_BYTES,
				checksumBytes, 0, nChecksumBytes);
		checksum.update(checksumBytes, 0, nChecksumBytes);
		LogUtils.writeUnsignedInt(data, checksum.getValue());
	}

	boolean around() : readNextEntry() {

		boolean r = false;
		try {
			r = proceed();
		} catch (DbChecksumException e) {
			// refined trace
			/*
			 * Tracer.trace(Level.INFO, env, "Found checksum exception while
			 * searching " + " for end of log. Last valid entry is at " +
			 * DbLsn.toString (DbLsn.makeLsn(readBufferFileNum,
			 * lastValidOffset)) + " Bad entry is at " +
			 * DbLsn.makeLsn(readBufferFileNum, currentEntryOffset));
			 */
		}
		return r;
	}

	boolean around(RandomAccessFile newFile, String fileName, FileManager fm)
			throws DatabaseException:
				readAndValidateFileHeader(newFile, fileName, fm) {
		try {
			return proceed(newFile, fileName, fm);
		} catch (DbChecksumException e) {

			/*
			 * Let this exception go as a checksum exception, so it sets the run
			 * recovery state correctly.
			 */
			fm.closeFileInErrorCase(newFile);
			throw new DbChecksumException(fm.envImpl, "Couldn't open file "
					+ fileName, e);
		}
	}

	before(ByteBuffer dataBuffer, FileReader fileReader) :
		readHeader3(dataBuffer, fileReader) {
		/* Get the checksum for this log entry. */
		fileReader.currentEntryChecksum = LogUtils.getUnsignedInt(dataBuffer);
	}

	// static final int LogManager.CHECKSUM_BYTES = 4; // size of checksum field
	//
	// static final int LogManager.HEADER_CHECKSUM_OFFSET = 0; // size of
	// checksum

	// field

	after(): staticinitialization(LogManager) {
		LogManager.HEADER_BYTES += LogManager.CHECKSUM_BYTES; // size of entry
		// header

		LogManager.PREV_BYTES = 4; // size of previous field

		LogManager.HEADER_CONTENT_BYTES = LogManager.HEADER_BYTES
				- LogManager.CHECKSUM_BYTES;

		LogManager.HEADER_ENTRY_TYPE_OFFSET += 4;

		LogManager.HEADER_VERSION_OFFSET += 4;

		LogManager.HEADER_PREV_OFFSET += 4;

		LogManager.HEADER_SIZE_OFFSET += 4;
	}

	after()returning(ByteBuffer buffer) : allocate() {

		/* Reserve 4 bytes at the head for the checksum. */
		buffer.position(LogManager.CHECKSUM_BYTES);
	}

	after(StringBuffer sb, PrintFileReader pfr) : hook_printChecksum(sb, pfr){
		sb.append("\" cksum=\"").append(pfr.currentEntryChecksum);
	}

	/**
	 * add properties to the file reader and initialize the checksum object
	 */
	/* For checking checksum on the read. */
	// TODO made public because protected intertype declarations are not allowed
	public ChecksumValidator FileReader.cksumValidator;

	private boolean FileReader.doValidateChecksum; // Validate checksums

	private boolean FileReader.alwaysValidateChecksum; // Validate for all

	// entry types

	/* True if this is the scavenger and we are expecting checksum issues. */
	// TODO made public because protected intertype declarations are not allowed
	public boolean FileReader.anticipateChecksumErrors;

	/**
	 * Reset the checksum and add the header bytes. This method must be called
	 * with the entry header data at the buffer mark.
	 */
	private void FileReader.startChecksum(ByteBuffer dataBuffer)
			throws DatabaseException {

		/* Move back up to the beginning of the cksum covered header. */
		cksumValidator.reset();
		int entryStart = threadSafeBufferPosition(dataBuffer);
		dataBuffer.reset();
		cksumValidator.update(env, dataBuffer, LogManager.HEADER_CONTENT_BYTES,
				anticipateChecksumErrors);

		/* Move the data buffer back to where the log entry starts. */
		threadSafeBufferPosition(dataBuffer, entryStart);
	}

	public long FileReader.currentEntryChecksum;

	/**
	 * Add the entry bytes to the checksum and check the value. This method must
	 * be called with the buffer positioned at the start of the entry.
	 */
	private void FileReader.validateChecksum(ByteBuffer entryBuffer)
			throws DatabaseException {

		cksumValidator.update(env, entryBuffer, currentEntrySize,
				anticipateChecksumErrors);
		cksumValidator.validate(env, currentEntryChecksum, readBufferFileNum,
				currentEntryOffset, anticipateChecksumErrors);
	}

	/**
	 * Whether to always validate the checksum, even for non-target entries.
	 */
	public void FileReader.setAlwaysValidateChecksum(boolean validate) {
		alwaysValidateChecksum = validate;
	}

	private boolean LogManager.doChecksumOnRead; // if true, do checksum on

	// read

	public boolean LogManager.getChecksumOnRead() {
		return doChecksumOnRead;
	}

	private ChecksumValidator validator = null;

	private long EntryHeader.checksum;

	public long EntryHeader.getChecksum() {
		return checksum;
	}

	static final int LogManager.CHECKSUM_BYTES = 4; // size of checksum field

	static final int LogManager.HEADER_CHECKSUM_OFFSET = 0; // size of checksum

}