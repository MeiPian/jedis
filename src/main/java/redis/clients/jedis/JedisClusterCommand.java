package redis.clients.jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.SafeEncoder;

public abstract class JedisClusterCommand<T> {

	private static final String NO_DISPATCH_MESSAGE = "No way to dispatch this command to Redis Cluster.";

	private JedisClusterConnectionHandler connectionHandler;
	private int maxAttempts;
	private ThreadLocal<Jedis> askConnection = new ThreadLocal<Jedis>();

	public JedisClusterCommand(JedisClusterConnectionHandler connectionHandler, int maxAttempts) {
		this.connectionHandler = connectionHandler;
		this.maxAttempts = maxAttempts;
	}

	public abstract T execute(Jedis connection);

	/**
	 * 全部交由子类实现
	 * 
	 * @param pipeline
	 * @param subKeys
	 * @return
	 */
	public List<Object> execute(Pipeline pipeline, List<String> subKeys) {
		throw new UnsupportedOperationException();
	}

	public T run(String key) {
		if (key == null) {
			throw new JedisClusterException(NO_DISPATCH_MESSAGE);
		}

		return runWithRetries(SafeEncoder.encode(key), this.maxAttempts, false, false);
	}

	public T run(int keyCount, String... keys) {
		if (keys == null || keys.length == 0) {
			throw new JedisClusterException(NO_DISPATCH_MESSAGE);
		}

		// For multiple keys, only execute if they all share the
		// same connection slot.
		if (keys.length > 1) {
			int slot = JedisClusterCRC16.getSlot(keys[0]);
			for (int i = 1; i < keyCount; i++) {
				int nextSlot = JedisClusterCRC16.getSlot(keys[i]);
				if (slot != nextSlot) {
					throw new JedisClusterException(
							"No way to dispatch this command to Redis Cluster " + "because keys have different slots.");
				}
			}
		}

		return runWithRetries(SafeEncoder.encode(keys[0]), this.maxAttempts, false, false);
	}

	public List<String> runWithMulti(int keyCount, String... keys) {
		List<String> result = new ArrayList<>();
		if (keys == null || keys.length == 0) {
			throw new JedisClusterException(NO_DISPATCH_MESSAGE);
		}
			Map<JedisPool, List<String>> poolKeysMap = getPoolKeyMap(Arrays.asList(keys));
			for (Map.Entry<JedisPool, List<String>> entry : poolKeysMap.entrySet()) {
				JedisPool pool = entry.getKey();
				List<String> subKeys = entry.getValue();
				if (subKeys == null || subKeys.isEmpty()) {
					continue;
				}
				Jedis jedis = null;
				Pipeline pipeline = null;
				List<Object> subResultList = new ArrayList<>();
				try {
					jedis = pool.getResource();
					pipeline = jedis.pipelined();
					subResultList = execute(pipeline, subKeys);
				} catch (Exception e) {
				} finally {
					if (pipeline != null)
						try {
							pipeline.close();
						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							if (jedis != null) {
								jedis.close();
							}
						}

				}
				for (Object object : subResultList) {
					if (object != null) {
						result.add(object.toString());
					}
				}

			}	
		return result;
	}

	public T runBinary(byte[] key) {
		if (key == null) {
			throw new JedisClusterException(NO_DISPATCH_MESSAGE);
		}

		return runWithRetries(key, this.maxAttempts, false, false);
	}

	public T runBinary(int keyCount, byte[]... keys) {
		if (keys == null || keys.length == 0) {
			throw new JedisClusterException(NO_DISPATCH_MESSAGE);
		}

		// For multiple keys, only execute if they all share the
		// same connection slot.
		if (keys.length > 1) {
			int slot = JedisClusterCRC16.getSlot(keys[0]);
			for (int i = 1; i < keyCount; i++) {
				int nextSlot = JedisClusterCRC16.getSlot(keys[i]);
				if (slot != nextSlot) {
					throw new JedisClusterException(
							"No way to dispatch this command to Redis Cluster " + "because keys have different slots.");
				}
			}
		}

		return runWithRetries(keys[0], this.maxAttempts, false, false);
	}

	public T runWithAnyNode() {
		Jedis connection = null;
		try {
			connection = connectionHandler.getConnection();
			return execute(connection);
		} catch (JedisConnectionException e) {
			throw e;
		} finally {
			releaseConnection(connection);
		}
	}

	private T runWithRetries(byte[] key, int attempts, boolean tryRandomNode, boolean asking) {
		if (attempts <= 0) {
			throw new JedisClusterMaxRedirectionsException("Too many Cluster redirections?");
		}

		Jedis connection = null;
		try {

			if (asking) {
				// TODO: Pipeline asking with the original command to make it
				// faster....
				connection = askConnection.get();
				connection.asking();

				// if asking success, reset asking flag
				asking = false;
			} else {
				if (tryRandomNode) {
					connection = connectionHandler.getConnection();
				} else {
					connection = connectionHandler.getConnectionFromSlot(JedisClusterCRC16.getSlot(key));
				}
			}

			return execute(connection);

		} catch (JedisNoReachableClusterNodeException jnrcne) {
			throw jnrcne;
		} catch (JedisConnectionException jce) {
			// release current connection before recursion
			releaseConnection(connection);
			connection = null;

			if (attempts <= 1) {
				// We need this because if node is not reachable anymore - we
				// need to finally initiate slots renewing,
				// or we can stuck with cluster state without one node in
				// opposite case.
				// But now if maxAttempts = 1 or 2 we will do it too often. For
				// each time-outed request.
				// TODO make tracking of successful/unsuccessful operations for
				// node - do renewing only
				// if there were no successful responses from this node last few
				// seconds
				this.connectionHandler.renewSlotCache();

				// no more redirections left, throw original exception, not
				// JedisClusterMaxRedirectionsException, because it's not MOVED
				// situation
				throw jce;
			}

			return runWithRetries(key, attempts - 1, tryRandomNode, asking);
		} catch (JedisRedirectionException jre) {
			// if MOVED redirection occurred,
			if (jre instanceof JedisMovedDataException) {
				// it rebuilds cluster's slot cache
				// recommended by Redis cluster specification
				this.connectionHandler.renewSlotCache(connection);
			}

			// release current connection before recursion or renewing
			releaseConnection(connection);
			connection = null;

			if (jre instanceof JedisAskDataException) {
				asking = true;
				askConnection.set(this.connectionHandler.getConnectionFromNode(jre.getTargetNode()));
			} else if (jre instanceof JedisMovedDataException) {
			} else {
				throw new JedisClusterException(jre);
			}

			return runWithRetries(key, attempts - 1, false, asking);
		} finally {
			releaseConnection(connection);
		}
	}

	private void releaseConnection(Jedis connection) {
		if (connection != null) {
			connection.close();
		}
	}

	private Map<JedisPool, List<String>> getPoolKeyMap(List<String> keys) {
		Map<JedisPool, List<String>> poolKeysMap = new LinkedHashMap<JedisPool, List<String>>();
		try {
			for (String key : keys) {
				int slot = JedisClusterCRC16.getSlot(key);
				JedisPool jedisPool = this.connectionHandler.getJedisPoolFromSlot(slot);
				if (poolKeysMap.containsKey(jedisPool)) {
					poolKeysMap.get(jedisPool).add(key);
				} else {
					List<String> subKeyList = new ArrayList<String>();
					subKeyList.add(key);
					poolKeysMap.put(jedisPool, subKeyList);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();

		}
		return poolKeysMap;
	}
}
