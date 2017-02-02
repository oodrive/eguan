package io.eguan.srv;

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

import io.eguan.srv.AbstractServer;
import io.eguan.srv.AbstractServerConfig;
import io.eguan.srv.DeviceTarget;
import io.eguan.utils.unix.UnixTarget;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

public abstract class TargetMgr<S extends Callable<Void>, T extends DeviceTarget, K extends AbstractServerConfig> {

    protected abstract AbstractServer<S, T, K> createServer();

    protected abstract void addTarget(AbstractServer<S, T, K> server, Map<File, Long> targets) throws IOException;

    protected abstract ClientBasicIops initClient() throws IOException;

    protected abstract UnixTarget createTarget(final File deviceFile, final int count) throws IOException;

    protected abstract String getTargetName(final File deviceFile);

    protected abstract void removeFiles();
}
