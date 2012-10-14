/**
 *Copyright [2009-2010] [dennis zhuang(killme2008@gmail.com)]
 *Licensed under the Apache License, Version 2.0 (the "License");
 *you may not use this file except in compliance with the License.
 *You may obtain a copy of the License at
 *             http://www.apache.org/licenses/LICENSE-2.0
 *Unless required by applicable law or agreed to in writing,
 *software distributed under the License is distributed on an "AS IS" BASIS,
 *WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *either express or implied. See the License for the specific language governing permissions and limitations under the License
 */
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

/**
 * Session locator base on hash(key) mod sessions.size().Standard hash strategy
 * 
 * @author dennis
 * 
 */
public class ArrayMemcachedSessionLocator extends
		AbstractMemcachedSessionLocator {

	public static final String ROUND_ROBIN = "round";
	public static final String RANDOM = "random";
	
	private HashAlgorithm hashAlgorighm;
	private transient volatile List<List<Session>> sessions;
	
	/**
	 * Store key to multiple server mapping
	 */
	private volatile ConcurrentHashMap<String, Vector<String>> keyServerMap;
	
	/**
	 * Store host to session mapping
	 */
	private volatile HashMap<String, List<Session>> hostSessionMap;
	
	/**
	 * Store key to last accessed server mapping
	 */
	private HashMap<String, Integer> keyAccessMap;
	
	/**
	 * Thread to fetch mapping from controller
	 */
	private MapFetchThread mft;
	
	/**
	 * Last session accessed. aka. Last server accessed!
	 */
	private int lastSessionIndex;
	
	/**
	 * If start controller
	 */
	private boolean connectController = true;
	
	/**
	 * Policy: random or round robin
	 */
	private String policy;
	
	public ArrayMemcachedSessionLocator(String controllerHostname, int controllerPort, boolean connectController, String policy, int interval) {
		this.hashAlgorighm = HashAlgorithm.NATIVE_HASH;
		this.connectController = connectController;
		
		if (connectController) {
			keyServerMap = new ConcurrentHashMap<String, Vector<String>>();
			keyAccessMap = new HashMap<String, Integer>();
			this.policy = policy;
			
			mft = new MapFetchThread(keyServerMap, controllerHostname, controllerPort, interval);
			mft.start();
		}
	}

	public ArrayMemcachedSessionLocator(HashAlgorithm hashAlgorighm, String controllerHostname, int controllerPort, boolean connectController, String policy, int interval) {
		this.hashAlgorighm = hashAlgorighm;
		this.connectController = connectController;
		
		if (connectController) {
			keyServerMap = new ConcurrentHashMap<String, Vector<String>>();
			keyAccessMap = new HashMap<String, Integer>();
			this.policy = policy;
			
			mft = new MapFetchThread(keyServerMap, controllerHostname, controllerPort, interval);
			mft.start();
		}
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
		
		if (this.connectController) {
			Vector<String> hosts = keyServerMap.get(key);
			
			/**
			 * Map contains key, get host from map
			 */
			if (hosts != null) {
				//System.out.println("Found key in keyServerMap!");
				
				if (this.policy.equals(ArrayMemcachedSessionLocator.RANDOM)) {
					String hostname = hosts.get(rand.nextInt(hosts.size()));
					List<Session> sessions = hostSessionMap.get(hostname);
					lastSessionIndex = sessionList.indexOf(sessions);
		
					Session session = getRandomSession(sessions);
					if (session != null) {
						return session;
					}
				}
				
				if (this.policy.equals(ArrayMemcachedSessionLocator.ROUND_ROBIN)) {
					Integer lastAccessedServer = keyAccessMap.get(key);
					String hostname = null;
					
					if (lastAccessedServer == null) {
						lastAccessedServer = 0;
						hostname = hosts.get(0);
					} else {
						int nextIndex = (lastAccessedServer + 1) % hosts.size();
						hostname = hosts.get(nextIndex);
						lastAccessedServer = nextIndex;
					}
					
					//System.out.println("Selected host is " + hostname);
					
					try {
						Thread.sleep(2);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					
					List<Session> sessions = hostSessionMap.get(hostname);
					
					if (sessions == null) {
						System.out.println("This shouldn't be null!");
					}
					
					lastSessionIndex = sessionList.indexOf(sessions);
					
					Session session = getRandomSession(sessions);
					if (session != null) {
						keyAccessMap.put(key, lastAccessedServer);
						return session;
					}
				}
			}
		}
		
		long start = this.getHash(size, key);
		lastSessionIndex = (int) start;
		
		List<Session> sessions = sessionList.get((int) start);
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
		return session;
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
		
		if (this.connectController) {
			hostSessionMap = new HashMap<String, List<Session>>();
		}
		
		for (List<Session> sessions : tmpList) {
			if (sessions != null && !sessions.isEmpty()) {
				Session session = sessions.get(0);
				
				if (this.connectController) {
					hostSessionMap.put(session.getRemoteSocketAddress().getAddress().getHostAddress(), sessions);
				}
				
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
	 * Stop map fetch thread if started
	 */
	public void stop() {
		if (this.connectController) {
			mft.halt();
		}
	}

	/**
	 * Get last accessed server index
	 */
	public int lastIndex() {
		return lastSessionIndex;
	}
}

/**
 * Thread to pull the map very 5 seconds
 * @author lihao
 *
 */
class MapFetchThread extends Thread {
	/**
	 * Ker server map
	 * Key --> Server ip list
	 */
	private ConcurrentHashMap<String, Vector<String>> keyServerMap;
	/**
	 * Controller host name
	 */
	private String controllerHost;
	/**
	 * Controller port number
	 */
	private int controllerPort;
	/**
	 * Should the thread be stopped
	 */
	private boolean stop = false;
	/**
	 * Interval to pull map
	 */
	private int interval = 5;
	
	/**
	 * Constructor
	 * @param keyServerMap Key server map from DB
	 * @param controllerHost Controller host name
	 * @param controllerPort Controller port number
	 */
	public MapFetchThread(ConcurrentHashMap<String, Vector<String>> keyServerMap, String controllerHost, int controllerPort, int interval) {
		this.keyServerMap = keyServerMap;
		this.controllerHost = controllerHost;
		this.controllerPort = controllerPort;
		this.interval = interval;
	}
	
	public void run() {
		Socket s = null;
		DataOutputStream dos = null;
		BufferedReader br = null;
		
		try {
			s = new Socket(controllerHost, controllerPort);
			br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			dos = new DataOutputStream(s.getOutputStream());
		} catch (UnknownHostException e) {
			System.err.println("Unknowned controller host!");
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
			System.err.println("IO error connecting to controller!");
			e.printStackTrace();
			System.exit(1);
		}
		
		System.out.println("Connected to controler!");
		
		while (!stop) {
			try {
				//Try to reconnect to controller!
				if (s == null) {
					s = new Socket(controllerHost, controllerPort);
					br = new BufferedReader(new InputStreamReader(s.getInputStream()));
					dos = new DataOutputStream(s.getOutputStream());
					System.out.println("Connected to controller!");
				}
				
				//Send request
				dos.write("2:\r\n".getBytes());
				//System.out.println("Request sent to controller!");
				
				//Read mapping
				String line = null;
				while ((line = br.readLine()) != null) {
					if (line.equals("")) {
						break;
					}
					String[] tokens = line.split("\t");
					String[] ips = tokens[1].split(",");
					
					Vector<String> IPVector = new Vector<String>();
					for (String ip : ips) {
						String[] iphost = ip.split(":");
						IPVector.add(iphost[0].trim());
					}
					
					keyServerMap.put(tokens[0].trim(), IPVector);
				}
				//System.out.println("Mapping received!");
				/*System.out.println("This mapping is:");
				for (String key : keyServerMap.keySet()) {
					System.out.print(key + ": ");
					for (String ip : keyServerMap.get(key)) {
						System.out.print(ip + ",");
					}
					System.out.println();
				}*/
				
			} catch (IOException e) {
				System.err.println("Error in writing to controller!");
				System.err.println("Try to reconnect to controller in "+ interval + " seconds!");
				try {
					s.close();
				} catch (Exception e1) {
				}
				s = null;
			} catch (Exception e) {
				System.err.println("Error in writing to controller!");
				System.err.println("Try to reconnect to controller in " + interval + " seconds!");
				try {
					s.close();
				} catch (IOException e1) {
				}
				s = null;
			}
			
			/**
			 * Wait for 5 seconds before next fetching request
			 */
			try {
				Thread.sleep(interval * 1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	/**
	 * Stop thread
	 */
	public void halt() {
		stop = true;
	}
}
