package org.apache.maven.plugin.nar;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/**
 * Run JNI tests. Quick and dirty addition of JNI tests in the phase 'test' to get the report from the emma coverage
 * tool.
 * 
 * @goal nar-test-jni
 * @phase test
 * @requiresProject
 * @requiresDependencyResolution test
 * @author Loic Lambert - oodrive
 */
public class NarTestJniMojo extends NarIntegrationTestMojo {

    /**
     * Set this to 'true' to skip running tests, but still compile them.
     * 
     * @parameter expression="${skipNarJniTests}"
     * @since 2.4
     */
    private boolean skipNarJniTests;

    /**
     * @return SurefirePlugin Returns the skipExec.
     */
    public boolean isSkipExec() {
        return this.skipNarJniTests;
    }

    /**
     * @param skipExec
     *            the skipExec to set
     */
    public void setSkipExec(boolean skipExec) {
        this.skipNarJniTests = skipExec;
    }

    @Override
    public void narExecute() throws MojoExecutionException, MojoFailureException {
        if ( skipNarJniTests ) {
            getLog().info( "Tests are skipped." );
            return;
        }
        // Force run of JNI tests even if skipNarTests is true 
        super.setSkipExec(false);
        super.narExecute();
    }

}
