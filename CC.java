package CC;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.*;
import java.util.stream.IntStream;


public class CC {
	private static List<LogEntry> log = new ArrayList<>();
	private static Map<Integer, Integer> transactionLogPointers = new HashMap<>();
	private static Map<Integer, Integer> waitCount = new HashMap<>();

	private static void rollbackTransaction(List<LogEntry> log, Map<Integer, Integer> transactionLogPointers, int transactionId, int[] db) {
		Integer logPointer = transactionLogPointers.get(transactionId);
		while (logPointer != null && logPointer >= 0) {
			LogEntry logEntry = log.get(logPointer);
			if (logEntry.writeValue != -1) {
				db[logEntry.address] = logEntry.readValue;
			}
			logPointer = logEntry.previousLogPointer;
		}
	}

	private static void unlockAll(ReentrantReadWriteLock[] locks) {
		for (ReentrantReadWriteLock lock : locks) {
			if (lock.isWriteLockedByCurrentThread()) {
				lock.writeLock().unlock();
			} else {
				int lockCount = lock.getReadHoldCount();
				for (int i = 0; i < lockCount; i++) {
					lock.readLock().unlock();
				}
			}
		}
	}

	private static void incrementWaitCount(List<String> schedule, Map<Integer, Integer> waitCount, ReentrantReadWriteLock[] locks, int i, int[] db) {
		waitCount.put(i, waitCount.getOrDefault(i, 0) + 1);
		if (waitCount.get(i) > schedule.size() - 1) {
			rollbackTransaction(log, transactionLogPointers, i, db);
			unlockAll(locks);
		}

	}

	static class LogEntry {
		int timestamp;
		int transactionId;
		int address;
		int writeValue;
		int readValue;
		int previousLogPointer;

		public LogEntry(int timestamp, int transactionId, int address, int writeValue, int readValue, int previousLogPointer) {
			this.timestamp = timestamp;
			this.transactionId = transactionId;
			this.address = address;
			this.writeValue = writeValue;
			this.readValue = readValue;
			this.previousLogPointer = previousLogPointer;
		}
	}

	public static int[] executeSchedule(int[] db, List<String> schedule) {
		log.clear();
		transactionLogPointers.clear();
		waitCount.clear();
		int timestamp = 0;
		ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[db.length];
		for (int i = 0; i < locks.length; i++) {
			locks[i] = new ReentrantReadWriteLock();
		}

		for (int i = 0; i < schedule.size(); i++) {
			String[] actions = schedule.get(i).split(";");
			boolean done;
			do {
				done = true;
				for (String action : actions) {
					Pattern pattern = Pattern.compile("(\\w)\\((\\d+)(?:,(\\d+))?\\)");
					Matcher matcher = pattern.matcher(action);
					if (matcher.find()) {
						String op = matcher.group(1);
						int address = Integer.parseInt(matcher.group(2));

						switch (op) {
							case "R":
								if (!locks[address].isWriteLocked()) {
									locks[address].readLock().lock();
									int value = db[address];
									log.add(new LogEntry(timestamp++, i, address, -1, value, transactionLogPointers.getOrDefault(i, -1)));
									transactionLogPointers.put(i, log.size() - 1);
									locks[address].readLock().unlock();
								} else {
									incrementWaitCount(schedule, waitCount, locks, i, db);
									done = false;
								}
								break;
							case "W":
								if (!locks[address].isWriteLockedByCurrentThread() && locks[address].writeLock().tryLock()) {
									int newValue = Integer.parseInt(matcher.group(3) != null ? matcher.group(3) : matcher.group(2));
									db[address] = newValue;
									log.add(new LogEntry(timestamp++, i, address, newValue, -1, transactionLogPointers.getOrDefault(i, -1)));
									transactionLogPointers.put(i, log.size() - 1);
									locks[address].writeLock().unlock();
								} else {
									incrementWaitCount(schedule, waitCount, locks, i, db);
									done = false;
								}
								break;
							case "A":
								rollbackTransaction(log, transactionLogPointers, i, db);
								unlockAll(locks);
								transactionLogPointers.remove(i);  // Remove aborted transaction's log pointer
								break;
							case "C":
								unlockAll(locks);
								transactionLogPointers.remove(i);  // Remove committed transaction's log pointer
								break;
							default:
								break;
						}
					}
				}
			} while (!done);
		}
		return db;
	}



	private static boolean tryReadLock(ReentrantReadWriteLock lock) {
		return !lock.isWriteLocked() && lock.readLock().tryLock();
	}

	private static boolean tryWriteLock(ReentrantReadWriteLock lock) {
		return !lock.isWriteLockedByCurrentThread() && lock.writeLock().tryLock();
	}


}
