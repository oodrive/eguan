package org.apache.maven.plugin.nar;

/**
 * NAR file layout for the oodrive projects. Do not take aol nor type into
 * account for the file layout.
 * 
 * @author oodrive
 */
public final class NarFileLayoutOodrive implements NarFileLayout {

	@Override
	public String getLibDirectory(String aol, String type) {
		return "lib";
	}

	@Override
	public String getIncludeDirectory() {
		return "include";
	}

	@Override
	public String getBinDirectory(String aol) {
		return "bin";
	}

}
