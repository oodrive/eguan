package com.oodrive.nuage.utils.unix;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
 * %%
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
 * #L%
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

import javax.annotation.concurrent.Immutable;

/**
 * Utility methods to handle Unix users. This is a very simple implementation for test purpose, based on the file
 * <code>/etc/passwd</code>. It may not work if the management of users is more complex; in this case, it may be better
 * to run the command '<code>id [username]</code>' and parse the command output.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
@Immutable
public final class UnixUser {

    private static final String USER_FILE = "/etc/passwd";

    private final String name;
    private final int uid;
    private final int gid;

    private UnixUser(final String name, final int uid, final int gid) {
        super();
        this.name = name;
        this.uid = uid;
        this.gid = gid;
    }

    /**
     * Gets the user information on the current user.
     * 
     * @return the user information from the current user.
     */
    public static UnixUser getCurrentUser() {
        return getUser(System.getProperty("user.name"));
    }

    /**
     * Gets the user information on the given user.
     * 
     * @param name
     *            name of the user to find.
     * @return the user found
     * @throws IllegalArgumentException
     *             if no user if found for the given name
     */
    public static UnixUser getUser(final String name) throws IllegalArgumentException {
        // parse the user list of the current host
        try {
            final File file = new File(USER_FILE);
            String line;
            try (BufferedReader is = new BufferedReader(new FileReader(file))) {
                while ((line = is.readLine()) != null) {
                    final StringTokenizer tokens = new StringTokenizer(line, ":");
                    if (tokens.nextToken().equals(name)) {
                        // Skip password
                        tokens.nextToken();
                        // uid
                        final int uid = Integer.valueOf(tokens.nextToken()).intValue();
                        // gid
                        final int gid = Integer.valueOf(tokens.nextToken()).intValue();

                        return new UnixUser(name, uid, gid);
                    }
                }
            }

            // User not found
            throw new IllegalArgumentException(name);
        }
        catch (final IOException e) {
            // Should be able to read the file
            throw new IllegalStateException("Failed to read " + USER_FILE, e);
        }
    }

    /**
     * Gets the user name.
     * 
     * @return the name of the user.
     */
    public final String getName() {
        return name;
    }

    /**
     * Gets the unix user id of the user.
     * 
     * @return the uid of the user.
     */
    public final int getUid() {
        return uid;
    }

    /**
     * Gets the unix group id of the user.
     * 
     * @return the gid of the user.
     */
    public final int getGid() {
        return gid;
    }

}
