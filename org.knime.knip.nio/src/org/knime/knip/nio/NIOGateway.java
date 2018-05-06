package org.knime.knip.nio;

import org.knime.scijava.core.ResourceAwareClassLoader;
import org.scijava.Context;
import org.scijava.io.handle.DataHandleService;
import org.scijava.io.location.LocationService;
import org.scijava.plugin.DefaultPluginFinder;
import org.scijava.plugin.PluginIndex;

public class NIOGateway {

	private static NIOGateway m_instance;

	private static DataHandleService m_handles;
	private static LocationService m_loc;
	private static Context m_context;

	private NIOGateway() {
		m_context = new Context(new PluginIndex(
				new DefaultPluginFinder(new ResourceAwareClassLoader(getClass().getClassLoader(), getClass()))));
	}

	/**
	 * @return singleton instance of {@link KNIPGateway}
	 */
	public static synchronized NIOGateway getInstance() {
		if (m_instance == null) {
			m_instance = new NIOGateway();
		}
		return m_instance;
	}

	/**
	 * @return singleton instance of {@link DataHandleService}
	 */
	public static DataHandleService handles() {
		if (m_handles == null) {
			m_handles = getInstance().m_context.getService(DataHandleService.class);
		}
		return m_handles;
	}

	public static LocationService locations() {
		if (m_loc == null) {
			m_loc = getInstance().m_context.getService(LocationService.class);
		}
		return m_loc;
	}
}
