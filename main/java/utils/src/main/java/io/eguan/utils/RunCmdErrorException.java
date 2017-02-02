package io.eguan.utils;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2017 Oodrive
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

public class RunCmdErrorException extends IOException {

    private static final long serialVersionUID = -5917367071543910593L;

    /** Return code of the command */
    private final int exitValue;

    public RunCmdErrorException(final int exitValue) {
        super();
        this.exitValue = exitValue;
    }

    public RunCmdErrorException(final String message, final int exitValue) {
        super(message);
        this.exitValue = exitValue;
    }

    public RunCmdErrorException(final Throwable cause, final int exitValue) {
        super(cause);
        this.exitValue = exitValue;
    }

    public RunCmdErrorException(final String message, final Throwable cause, final int exitValue) {
        super(message, cause);
        this.exitValue = exitValue;
    }

    /**
     * Gets the exit value of the command.
     * 
     * @return exit value of the command
     */
    public final int getExitValue() {
        return exitValue;
    }

}
