package com.oodrive.nuage.nbdsrv;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2015 Oodrive
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

import java.io.IOException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.oodrive.nuage.nbdsrv.packet.NbdException;

/**
 * Represent a connection's phase. To run the phase, the execute() method is called and it's implemented by each
 * sub-class.
 * 
 * @author oodrive
 * @author ebredzinski
 */
abstract class PhaseAbstract implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhaseAbstract.class);

    /** The connection this phase is a part of. */
    private final ClientConnection connection;
    /** The name of the Thread */
    private final String threadName;

    public PhaseAbstract(final ClientConnection client) {
        this.connection = client;
        this.threadName = "NBD connection from " + client.getRemoteAddress();
    }

    /**
     * Gets the current connection.
     * 
     * @return the {@link ClientConnection}
     */
    protected final ClientConnection getConnection() {
        return connection;
    }

    abstract boolean execute() throws NbdException, IOException;

    @Override
    public final Boolean call() throws Exception {

        final Thread myThread = Thread.currentThread();
        final String prevThreadName = myThread.getName();

        myThread.setName(threadName);
        try {
            if (execute()) {
                connection.enableRead();
                return true;
            }
            else {
                connection.close();
                LOGGER.debug("Client disconnection request");
                return false;
            }
        }
        catch (final Throwable t) {
            connection.close();
            LOGGER.error("Exception thrown", t);
            return false;
        }
        finally {
            myThread.setName(prevThreadName);
        }
    }

}
