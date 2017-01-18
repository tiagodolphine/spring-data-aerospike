/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.aerospike.cache;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.cache.transaction.AbstractTransactionSupportingCacheManager;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.convert.AerospikeData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * {@link CacheManager} implementation for Aerospike. By default {@link AerospikeCache}s
 * will be lazily initialized for each {@link #getCache(String)} request unless a set of
 * predefined cache names is provided. <br>
 * <br>
 * Setting {@link #setTransactionAware(boolean)} to <code>true</code> will force Caches to
 * be decorated as {@link TransactionAwareCacheDecorator} so values will only be written
 * to the cache after successful commit of surrounding transaction.
 * 
 * @author Venil Noronha
 */
public class AerospikeCacheManager extends AbstractTransactionSupportingCacheManager {

	protected static final String DEFAULT_SET_NAME = "aerospike";

	private final AerospikeClient aerospikeClient;
	private final AerospikeConverter aerospikeConverter;
	private final String namespace;
	private final Set<String> configuredCacheNames;

	public Map<String, Long> getConfiguredCaches() {
		return configuredCaches;
	}

	public void setConfiguredCaches(Map<String, Long> configuredCaches) {
		this.configuredCaches = configuredCaches;
	}

	private Map<String, Long> configuredCaches;

	public Long getDefaultExpiration() {
		return defaultExpiration;
	}

	public void setDefaultExpiration(Long defaultExpiration) {
		this.defaultExpiration = defaultExpiration;
	}

	private Long defaultExpiration = TimeUnit.MINUTES.toSeconds(1);


	/**
	 * Create a new {@link AerospikeCacheManager} instance with no caches and with the
	 * set name "aerospike".
	 * 
	 * @param aerospikeClient the {@link AerospikeClient} instance.
	 */
	public AerospikeCacheManager(AerospikeClient aerospikeClient) {
		this(aerospikeClient, Collections.<String>emptyList());
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance with no caches and with the
	 * specified set name.
	 * 
	 * @param aerospikeClient the {@link AerospikeClient} instance.
	 * @param setName the set name.
	 */
	public AerospikeCacheManager(AerospikeClient aerospikeClient, String setName) {
		this(aerospikeClient, Collections.<String>emptyList(), null,  setName);
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance with the specified caches and
	 * with the set name "aerospike".
	 * 
	 * @param aerospikeClient the {@link AerospikeClient} instance.
	 * @param cacheNames the default caches to create.
	 */
	public AerospikeCacheManager(AerospikeClient aerospikeClient,
			Collection<String> cacheNames) {
		this(aerospikeClient, cacheNames, null, DEFAULT_SET_NAME);
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance with the specified caches and
	 * with the set name "aerospike".
	 *
	 * @param aerospikeClient the {@link AerospikeClient} instance.
	 * @param caches the default caches with expiration to create.
	 * @param namespace the namespace
	 */
	public AerospikeCacheManager(AerospikeClient aerospikeClient,
								 Map<String, Long> caches, String namespace) {
		this(aerospikeClient,  Collections.<String>emptyList(), caches, namespace);
	}

	/**
	 * Create a new {@link AerospikeCacheManager} instance with the specified caches and
	 * with the specified set name.
	 * 
	 * @param aerospikeClient the {@link AerospikeClient} instance.
	 * @param cacheNames the default caches to create.
	 * @param namespace the set name.
	 */
	public AerospikeCacheManager(AerospikeClient aerospikeClient,
			Collection<String> cacheNames,  Map<String, Long> caches, String namespace) {
		Assert.notNull(aerospikeClient, "AerospikeClient must not be null");
		Assert.notNull(cacheNames, "Cache names must not be null");
		Assert.notNull(namespace, "Namespace name must not be null");
		this.aerospikeClient = aerospikeClient;
		this.aerospikeConverter = new MappingAerospikeConverter();
		this.namespace = namespace;
		this.configuredCacheNames = new LinkedHashSet<String>(cacheNames);
		this.configuredCaches = caches;
	}

	@Override
	protected Collection<? extends Cache> loadCaches() {
		List<AerospikeCache> caches = new ArrayList<AerospikeCache>();
		if(!CollectionUtils.isEmpty(configuredCaches)){
			for(Map.Entry<String, Long> entry : configuredCaches.entrySet()){
				String cacheName = entry.getKey();
				Long expiration = entry.getValue();
				caches.add(createCache(cacheName, expiration != null ? expiration :  defaultExpiration));
			};
		}else {
			for (String cacheName : configuredCacheNames) {
				caches.add(createCache(cacheName, defaultExpiration));
			}
		}
		return caches;
	}

	@Override
	protected Cache getMissingCache(String cacheName) {
		Long expiration = configuredCaches.get(cacheName);
		return createCache(cacheName, expiration!=null ? expiration :  defaultExpiration);
	}

	protected AerospikeCache createCache(String cacheName, long expiration) {
		return new AerospikeSerializingCache(namespace, cacheName, expiration , aerospikeClient);
	}

	@Override
	public Cache getCache(String name) {
		Cache cache = lookupAerospikeCache(name);
		if (cache != null) {
			return cache;
		}
		else {
			Cache missingCache = getMissingCache(name);
			if (missingCache != null) {
				addCache(missingCache);
				return lookupAerospikeCache(name);  // may be decorated
			}
			return null;
		}
	}

	protected Cache lookupAerospikeCache(String name) {
		return lookupCache(this.namespace + ":"+ name);
	}

	@Override
	protected Cache decorateCache(Cache cache) {
		if (isCacheAlreadyDecorated(cache)) {
			return cache;
		}
		return super.decorateCache(cache);
	}

	protected boolean isCacheAlreadyDecorated(Cache cache) {
		return isTransactionAware() && cache instanceof TransactionAwareCacheDecorator;
	}

	public class AerospikeSerializingCache extends AerospikeCache {

		public AerospikeSerializingCache(String namespace, String setName, long expiration, AerospikeClient aerospikeClient) {
			super(namespace, setName, aerospikeClient, expiration);
		}

		@Override
		public <T> T get(Object key, Class<T> type) {
			Key dbKey = getKey(key);
			Record record =  client.get(null, dbKey);
			if (record != null) {
				AerospikeData data = AerospikeData.forRead(dbKey, null);
				data.setRecord(record);
				T value = aerospikeConverter.read(type,  data);
				return value;
			}
			return null;
		}

		@Override
		public ValueWrapper get(Object key) {
			Object value = get(key, Object.class);
			return (value != null ? new SimpleValueWrapper(value) : null);
		}

		private void serializeAndPut(WritePolicy writePolicy, Object key, Object value) {
			AerospikeData data = AerospikeData.forWrite(set);
			data.setID(key.toString());
			aerospikeConverter.write(value, data);
			client.put(writePolicy, getKey(key), data.getBinsAsArray());
		}

		@Override
		public void put(Object key, Object value) {
			serializeAndPut(create, key, value);
		}

		@Override
		public ValueWrapper putIfAbsent(Object key, Object value) {
			serializeAndPut(createOnly, key, value);
			return get(key);
		}
	}

}
