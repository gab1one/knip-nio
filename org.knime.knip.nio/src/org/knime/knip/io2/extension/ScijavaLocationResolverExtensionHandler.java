package org.knime.knip.io2.extension;

import java.util.ArrayList;
import java.util.List;

import org.eclipse.core.runtime.IConfigurationElement;
import org.eclipse.core.runtime.IExtensionPoint;
import org.eclipse.core.runtime.IExtensionRegistry;
import org.eclipse.core.runtime.Platform;
import org.knime.core.node.NodeLogger;
import org.scijava.io.location.LocationResolver;

public class ScijavaLocationResolverExtensionHandler {
	   private static final NodeLogger LOGGER = NodeLogger
	            .getLogger(ScijavaLocationResolverExtensionHandler.class);

	    private ScijavaLocationResolverExtensionHandler() {
	        // utility class
	    }

	    /** The id of the IFormatReaderExtPoint extension point. */
	    public static final String EXT_POINT_ID = "org.knime.knip.io2.LocationResolver";

	    /**
	     * The attribute of the iformatreader view extension point pointing to the
	     * factory class
	     */
	    public static final String EXT_POINT_ATTR_DF = "LocationResolver";

	    public static List<LocationResolver> getResolvers() {

	        List<LocationResolver> resolvers = new ArrayList<>();

	        try {
	            final IExtensionRegistry registry = Platform.getExtensionRegistry();
	            final IExtensionPoint point =
	                    registry.getExtensionPoint(EXT_POINT_ID);
	            if (point == null) {
	                LOGGER.error("Invalid extension point: " + EXT_POINT_ID);
	                throw new IllegalStateException("ACTIVATION ERROR: "
	                        + " --> Invalid extension point: " + EXT_POINT_ID);
	            }
	            for (final IConfigurationElement elem : point
	                    .getConfigurationElements()) {
	                final String operator = elem.getAttribute(EXT_POINT_ATTR_DF);
	                final String decl =
	                        elem.getDeclaringExtension().getUniqueIdentifier();

	                if (operator == null || operator.isEmpty()) {
	                    LOGGER.error("The extension '" + decl
	                            + "' doesn't provide the required attribute '"
	                            + EXT_POINT_ATTR_DF + "'");
	                    LOGGER.error("Extension " + decl + " ignored.");
	                    continue;
	                }

	                try {
	                    final LocationResolver resolver =
	                            (LocationResolver) elem
						        .createExecutableExtension(EXT_POINT_ATTR_DF);
	                    resolvers.add(resolver);
	                } catch (final Throwable t) {
	                    LOGGER.error("Problems during initialization of "
	                            + "ScifioFormatExtensionPoint (with id '"
	                            + operator + "'.)");
	                    if (decl != null) {
	                        LOGGER.error("Extension " + decl + " ignored.", t);
	                    }
	                }
	            }
	        } catch (final Exception e) {
	            LOGGER.error("Exception while registering "
	                    + "ScifioFormat extensions");
	        }

	        return resolvers;

	    }
}
