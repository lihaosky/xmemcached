package net.rubyeye.xmemcached.impl;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import net.rubyeye.xmemcached.HashAlgorithm;
import net.rubyeye.xmemcached.networking.MemcachedSession;

import com.google.code.yanf4j.core.Session;

public class ThreeRandomMemcachedSessionLocator extends AbstractMemcachedSessionLocator {
	
	private HashAlgorithm hashAlgorighm;
	private transient volatile List<List<Session>> sessions;
	
	/**
	 * Store key to multiple server mapping
	 */
	private volatile ConcurrentHashMap<String, Vector<Session>> keySessionMap;

	/**
	 * Last session accessed. aka. Last server accessed!
	 */
	private int lastSessionIndex;

	/**
	 * Number of copies to replicate
	 */
	private int copyNum;
	
	public ThreeRandomMemcachedSessionLocator(int copyNum) {
		this.hashAlgorighm = HashAlgorithm.NATIVE_HASH;
		this.copyNum = copyNum;
		keySessionMap = new ConcurrentHashMap<String, Vector<Session>>();
	}

	public ThreeRandomMemcachedSessionLocator(HashAlgorithm hashAlgorighm, int copyNum) {
		this.hashAlgorighm = hashAlgorighm;
		this.copyNum = copyNum;
		keySessionMap = new ConcurrentHashMap<String, Vector<Session>>();
	}
	
	public final void setHashAlgorighm(HashAlgorithm hashAlgorighm) {
		this.hashAlgorighm = hashAlgorighm;
	}

	public final long getHash(int size, String key) {
		long hash = this.hashAlgorighm.hash(key);
		return hash % size;
	}

	final Random rand = new Random();

	/**
	 * Find session by key
	 * If key presented in map, get session from map
	 * Else pick session by hashing
	 */
	public final Session getSessionByKey(final String key, boolean isSet) {
		if (this.sessions == null || this.sessions.size() == 0) {
			return null;
		}
		// Copy on read
		List<List<Session>> sessionList = this.sessions;
		int size = sessionList.size();
		if (size == 0) {
			return null;
		}
		
		/**
		 * Get a key, find random session from key session list
		 */
		if (isSet == false) {
			Vector<Session> sessionVector = keySessionMap.get(key);
			return sessionVector.get(rand.nextInt(sessionVector.size()));
		} else {
			/**
			 * Set a key, find a new session if copyNum is not reached.
			 */
			Vector<Session> sessionVector = keySessionMap.get(key);
			/**
			 * Not stored in any session, find a random session
			 */
			if (sessionVector == null) {
				long start = this.getHash(size, key);
				List<Session> sessions = sessionList.get((int)start);
				Session session = getRandomSession(sessions);
				
				// If it is not failure mode,get next available session
				if (!this.failureMode && (session == null || session.isClosed())) {
					long next = this.getNext(size, start);
					while ((session == null || session.isClosed()) && next != start) {
						sessions = sessionList.get((int) next);
						next = this.getNext(size, next);
						session = getRandomSession(sessions);
					}
				}
				
				sessionVector = new Vector<Session>();
				sessionVector.add(session);
				keySessionMap.put(key, sessionVector);
				
				return session;
			} else if (sessionVector.size() < copyNum && copyNum <= sessionList.size()) {
				List<Session> sessions = sessionList.get(rand.nextInt(sessionList.size()));
				Session session = sessions.get(rand.nextInt(sessions.size()));
				while (sessionVector.contains(session) == false) {
					sessions = sessionList.get(rand.nextInt(sessionList.size()));
					session = sessions.get(rand.nextInt(sessions.size()));
				}
				
				sessionVector.add(session);
				return session;
			} else {
				return sessionVector.get(rand.nextInt(sessionVector.size()));
			}
		}
	}

	
	public int findSessionNumByKey(final String key) {
		List<List<Session>> sessionList = this.sessions;
		int size = sessionList.size();
		return (int)(this.getHash(size, key));
	}
	
	private Session getRandomSession(List<Session> sessions) {
		if (sessions == null || sessions.isEmpty())
			return null;
		return sessions.get(rand.nextInt(sessions.size()));
	}

	public final long getNext(int size, long start) {
		if (start == size - 1) {
			return 0;
		} else {
			return start + 1;
		}
	}

	public final void updateSessions(final Collection<Session> list) {
		if (list == null || list.isEmpty()) {
			this.sessions = Collections.emptyList();
			return;
		}
		Collection<Session> copySessions = list;
		List<List<Session>> tmpList = new ArrayList<List<Session>>();
		Session target = null;
		List<Session> subList = null;
		for (Session session : copySessions) {
			if (target == null) {
				target = session;
				subList = new ArrayList<Session>();
				subList.add(target);
			} else {
				if (session.getRemoteSocketAddress().equals(
						target.getRemoteSocketAddress())) {
					subList.add(session);
				} else {					
					tmpList.add(subList);
					target = session;
					subList = new ArrayList<Session>();
					subList.add(target);
				}
			}
		}

		// The last one
		if (subList != null) {
			tmpList.add(subList);
		}

		List<List<Session>> newSessions = new ArrayList<List<Session>>(
				tmpList.size() * 2);
		
		
		for (List<Session> sessions : tmpList) {
			if (sessions != null && !sessions.isEmpty()) {
				Session session = sessions.get(0);
				
				if (session instanceof MemcachedTCPSession) {
					int weight = ((MemcachedSession) session).getWeight();
					for (int i = 0; i < weight; i++) {
						newSessions.add(sessions);
					}
				} else {
					newSessions.add(sessions);
				}
			}

		}
		this.sessions = newSessions;
	}

	/**
	 * Get last accessed server index
	 */
	public int lastIndex() {
		return lastSessionIndex;
	}

	@Override
	public void stop() {
		
	}

}
