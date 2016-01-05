package io.eguan.nrs;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2016 Oodrive
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

import io.eguan.configuration.ValidConfigurationContext;
import io.eguan.nrs.NrsConfigurationContext;

import org.junit.After;
import org.junit.Before;
import org.junit.runners.model.InitializationError;

/**
 * Default test context.
 * 
 * @author oodrive
 * @author llambert
 * 
 */
public abstract class TestAbstractNrsConfigurationContext extends ValidConfigurationContext {

    private final ContextTestHelper<NrsConfigurationContext> testHelper;

    // Common definitions
    protected static final String NRS_TMPDIR_PREFIX = "tmpNrsStorage";

    TestAbstractNrsConfigurationContext(final ContextTestHelper<NrsConfigurationContext> testHelper) {
        super();
        this.testHelper = testHelper;
    }

    @Before
    public final void setUpClass() throws InitializationError {
        testHelper.setUp();
    }

    @After
    public final void tearDownClass() throws InitializationError {
        testHelper.tearDown();
    }

    @Override
    public final ContextTestHelper<?> getTestHelper() {
        return testHelper;
    }

}
