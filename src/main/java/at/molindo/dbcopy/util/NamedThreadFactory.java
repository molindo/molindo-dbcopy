/**
 * Copyright 2010 Molindo GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package at.molindo.dbcopy.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

	private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(NamedThreadFactory.class);

	private final String _prefix;
	private final AtomicInteger _counter = new AtomicInteger();

	public NamedThreadFactory(String prefix) {
		if (prefix == null) {
			throw new NullPointerException("prefix");
		}
		_prefix = prefix;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r, _prefix + _counter.getAndIncrement());

		if (log.isTraceEnabled()) {
			log.trace("created thread " + t.getName());
		}

		return t;
	}
}
