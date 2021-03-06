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
package net.rubyeye.xmemcached;

import java.util.Collection;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import com.google.code.yanf4j.core.Session;

/**
 * Session locator.Find session by key.
 * 
 * @author dennis
 * 
 */
public interface MemcachedSessionLocator {
	/**
	 * Returns a session by special key.
	 * 
	 * @param key
	 * @return
	 */
	public Session getSessionByKey(final String key, boolean isSet);

	/**
	 * Update sessions when session was added or removed.
	 * 
	 * @param list
	 *            The newer sessions
	 */
	public void updateSessions(final Collection<Session> list);

	/**
	 * Configure failure mode
	 * 
	 * @param failureMode
	 *            true is using failure mode
	 */
	public void setFailureMode(boolean failureMode);
	
	/**
	 * @param key
	 * @return Session number
	 */
	public int findSessionNumByKey(final String key);
	
	public void stop();
	
	public int lastIndex();
}
