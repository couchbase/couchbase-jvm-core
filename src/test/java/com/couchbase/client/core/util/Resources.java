package com.couchbase.client.core.util;

import java.io.InputStream;

/**
 * Helper class for various resource handling mechanisms.
 *
 * @author Michael Nitschinger
 * @since 1.0
 */
public class Resources {

    /**
     * Reads a file from the resources folder (in the same path as the requesting test class).
     *
     * The class will be automatically loaded relative to the namespace and converted to a string.
     *
     * @param filename the filename of the resource.
     * @param clazz the reference class.
     * @return the loaded string.
     */
    public static String read(final String filename, final Class<?> clazz) {
        String path = "/" + clazz.getPackage().getName().replace(".", "/") + "/" + filename;
        InputStream stream = clazz.getResourceAsStream(path);
        java.util.Scanner s = new java.util.Scanner(stream).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }
}
