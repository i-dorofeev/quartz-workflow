package ru.dorofeev.sandbox.quartzworkflow;

import liquibase.resource.ClassLoaderResourceAccessor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

class CustomClassLoaderResourceAccessor extends ClassLoaderResourceAccessor {

	CustomClassLoaderResourceAccessor(ClassLoader classLoader) {
		super(classLoader);
	}

	// этот код идентичен коду ClassLoaderResourceAccessor за исключением закомментированной строчки
	// она, как оказалось, очень ресурсоемка (2 сек)
	@SuppressWarnings("Convert2Diamond")
	@Override
	public Set<InputStream> getResourcesAsStream(String path) throws IOException {
		Enumeration<URL> resources = toClassLoader().getResources(path);
		if (resources == null || !resources.hasMoreElements()) {
			return null;
		}
		Set<String> seenUrls = new HashSet<String>();
		Set<InputStream> returnSet = new HashSet<InputStream>();
		while (resources.hasMoreElements()) {
			URL url = resources.nextElement();
			if (seenUrls.contains(url.toExternalForm())) {
				continue;
			}
			seenUrls.add(url.toExternalForm());
			//	LogFactory.getInstance().getLog().debug("Opening "+url.toExternalForm()+" as "+path);

			URLConnection connection = url.openConnection();
			connection.setUseCaches(false);
			InputStream resourceAsStream = connection.getInputStream();
			if (resourceAsStream != null) {
				returnSet.add(resourceAsStream);
			}
		}

		return returnSet;
	}
}
