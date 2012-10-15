package net.rubyeye.xmemcached.impl;

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
	private static volatile ConcurrentHashMap<String, Vector<String>> keyHostMap = new ConcurrentHashMap<String, Vector<String>>();;

	private HashMap<String, List<Session>> hostSessionMap;
	private Vector<String> hosts;
	
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
		hostSessionMap = new HashMap<String, List<Session>>();
		hosts = new Vector<String>();
	}

	public ThreeRandomMemcachedSessionLocator(HashAlgorithm hashAlgorighm, int copyNum) {
		this.hashAlgorighm = hashAlgorighm;
		this.copyNum = copyNum;
		hostSessionMap = new HashMap<String, List<Session>>();
		hosts = new Vector<String>();
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
			Vector<String> hostVector = keyHostMap.get(key);
			List<Session> sessions = hostSessionMap.get(hostVector.get(rand.nextInt(hostVector.size())));
			return sessions.get(rand.nextInt(sessions.size()));
		} else {
			/**
			 * Set a key, find a new session if copyNum is not reached.
			 */
			Vector<String> hostVector = keyHostMap.get(key);
			/**
			 * Not stored in any session, find a random session
			 */
			if (hostVector == null) {
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
				
				
				hostVector = new Vector<String>();
				hostVector.add(session.getRemoteSocketAddress().getAddress().getHostAddress());
				keyHostMap.put(key, hostVector);
				
				return session;
			} else if (hostVector.size() < copyNum && copyNum <= sessionList.size()) {
				String hostname = hosts.get(rand.nextInt(hosts.size()));
				
				while (hostVector.contains(hostname) == true) {
					hostname = hosts.get(rand.nextInt(hosts.size()));
				}
				
				hostVector.add(hostname);
				List<Session> sessions = hostSessionMap.get(hostname);
				return sessions.get(rand.nextInt(sessions.size()));
			} else {
				String hostname = hostVector.get(rand.nextInt(hostVector.size()));
				List<Session> sessions = hostSessionMap.get(hostname);
				return sessions.get(rand.nextInt(sessions.size()));
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
				
				hostSessionMap.put(session.getRemoteSocketAddress().getAddress().getHostAddress(), sessions);
				hosts.add(session.getRemoteSocketAddress().getAddress().getHostAddress());
				
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
