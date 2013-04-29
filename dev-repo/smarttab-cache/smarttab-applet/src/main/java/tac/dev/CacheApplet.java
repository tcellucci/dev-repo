package tac.dev;

import java.applet.Applet;
import java.net.URL;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.hazelcast.config.ClasspathXmlConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.UrlXmlConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class CacheApplet extends Applet {
	private static Logger logger = Logger
			.getLogger(CacheApplet.class.getName());
	private static final long serialVersionUID = 1L;
	private HazelcastInstance hc;
	private Config hazelConfig = null;

	@Override
	public void init() {
		super.init();
		logger.info("initting applet " + super.getName());
		String configUrlParam = getParameter("config-url");
		if (configUrlParam != null) {
			try {
				URL configUrl = new URL(super.getDocumentBase(), configUrlParam);
				logger.info("loading hazelcast config from url " + configUrl);
				hazelConfig = new UrlXmlConfig(configUrl);
			} catch (Exception e) {
				logger.warning("failed to load hazelcast config");
			}
		}
		if (hazelConfig == null) {
			logger.warning("loading hazelcast config from default configuration");
			hazelConfig = new ClasspathXmlConfig("hazelcast-default.xml");
		}
		hazelConfig.setProperty("hazelcast.version.check.enabled", "false");
		hc = Hazelcast.newHazelcastInstance(hazelConfig);
	}

	@Override
	public void start() {
		logger.info("starting applet " + super.getName());
		if (hc == null) {
			hc = Hazelcast.newHazelcastInstance(hazelConfig);		
		}
		super.start();
	}

	public ConcurrentMap<String, Object> getMap(String name) {
		return hc.getMap(name);
	}

	public <T> BlockingQueue<T> getQueue(String name) {
		return hc.<T>getQueue(name);
	}
	
	public String getValue(String mapName, String key) {
		return (String) hc.getMap(mapName).get(key);
	}

	public String setValue(String mapName, String key, String value) {
		return (String) hc.getMap(mapName).put(key, value);
	}
	
	public HazelcastInstance getHazelcastInstance() {
		return hc;
	}

	@Override
	public void stop() {
		logger.info("stopping applet " + super.getName());
		super.stop();
	}

	@Override
	public void destroy() {
		logger.info("destroying applet " + super.getName());
		if (hc != null) {
			hc.getLifecycleService().kill();
		}
		hc = null;
		super.destroy();
	}

}
