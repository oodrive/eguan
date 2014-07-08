package org.apache.maven.plugin.nar;

import java.io.File;
import java.io.IOException;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugin.logging.Log;
import org.apache.maven.project.MavenProject;
import org.apache.maven.project.MavenProjectHelper;
import org.codehaus.plexus.archiver.manager.ArchiverManager;
import org.codehaus.plexus.util.FileUtils;

/**
 * NAR layout for oodrive projects. Pack all the binaries in the same NAR (bin
 * and lib directories).
 * <p>
 * TODO separate dev binaries / noarch files from runtime binaries / noarch
 * files.
 * 
 * @author oodrive
 * @author llambert
 */
public final class NarLayoutOodrive extends AbstractNarLayout {

	private final NarFileLayout fileLayout;

	public NarLayoutOodrive(Log log) {
		super(log);
		this.fileLayout = new NarFileLayoutOodrive();
	}

	private File getAolDirectory(File baseDir, String artifactId,
			String version, String aol, String type) {
		return new File(baseDir, artifactId + "-" + version + "-" + aol + "-bin");
	}

	@Override
	public File getNoArchDirectory(File baseDir, String artifactId,
			String version) throws MojoExecutionException, MojoFailureException {
		return new File(baseDir, artifactId + "-" + version + "-"
				+ NarConstants.NAR_NO_ARCH);
	}

	@Override
	public File getLibDirectory(File baseDir, String artifactId,
			String version, String aol, String type)
			throws MojoExecutionException, MojoFailureException {
//		if (type.equals(Library.EXECUTABLE)) {
//			throw new MojoExecutionException(
//					"NAR: for type EXECUTABLE call getBinDirectory instead of getLibDirectory");
//		}

		File dir = getAolDirectory(baseDir, artifactId, version, aol, type);
		dir = new File(dir, fileLayout.getLibDirectory(aol, type));
		return dir;
	}

	@Override
	public File getIncludeDirectory(File baseDir, String artifactId,
			String version) throws MojoExecutionException, MojoFailureException {
		return new File(getNoArchDirectory(baseDir, artifactId, version),
				fileLayout.getIncludeDirectory());
	}

	@Override
	public File getBinDirectory(File baseDir, String artifactId,
			String version, String aol) throws MojoExecutionException,
			MojoFailureException {
		File dir = getAolDirectory(baseDir, artifactId, version, aol,
				Library.EXECUTABLE);
		dir = new File(dir, fileLayout.getBinDirectory(aol));
		return dir;
	}

	@Override
	public void attachNars(File baseDir, ArchiverManager archiverManager,
			MavenProjectHelper projectHelper, MavenProject project,
			NarInfo narInfo) throws MojoExecutionException,
			MojoFailureException {
		if (getNoArchDirectory(baseDir, project.getArtifactId(),
				project.getVersion()).exists()) {
			attachNar(
					archiverManager,
					projectHelper,
					project,
					NarConstants.NAR_NO_ARCH,
					getNoArchDirectory(baseDir, project.getArtifactId(),
							project.getVersion()), "*/**");
			narInfo.setNar(null, NarConstants.NAR_NO_ARCH, project.getGroupId()
					+ ":" + project.getArtifactId() + ":"
					+ NarConstants.NAR_TYPE + ":" + NarConstants.NAR_NO_ARCH);
		}

		// list all directories in basedir, scan them for classifiers
		String[] subDirs = baseDir.list();
		for (int i = 0; (subDirs != null) && (i < subDirs.length); i++) {
			String artifactIdVersion = project.getArtifactId() + "-"
					+ project.getVersion();

			// skip entries not belonging to this project
			if (!subDirs[i].startsWith(artifactIdVersion))
				continue;

			String classifier = subDirs[i]
					.substring(artifactIdVersion.length() + 1);

			// skip noarch here
			if (classifier.equals(NarConstants.NAR_NO_ARCH))
				continue;

			File dir = new File(baseDir, subDirs[i]);
			attachNar(archiverManager, projectHelper, project, classifier, dir,
					"*/**");

			int lastDash = classifier.lastIndexOf('-');
			String type = classifier.substring(lastDash + 1);
			AOL aol = new AOL(classifier.substring(0, lastDash - 1));

			if (type.equals(Library.EXECUTABLE)) {
				if (narInfo.getBinding(aol, null) == null) {
					narInfo.setBinding(aol, Library.EXECUTABLE);
				}
				if (narInfo.getBinding(null, null) == null) {
					narInfo.setBinding(null, Library.EXECUTABLE);
				}
			} else {
				// and not set or override if SHARED
				if ((narInfo.getBinding(aol, null) == null)
						|| !type.equals(Library.SHARED)) {
					narInfo.setBinding(aol, type);
				}
				// and not set or override if SHARED
				if ((narInfo.getBinding(null, null) == null)
						|| !type.equals(Library.SHARED)) {
					narInfo.setBinding(null, type);
				}
			}

			narInfo.setNar(null, type,
					project.getGroupId() + ":" + project.getArtifactId() + ":"
							+ NarConstants.NAR_TYPE + ":" + "${aol}" + "-"
							+ type);
		}
	}

	@Override
	public void unpackNar(File unpackDirectory,
			ArchiverManager archiverManager, File file, String os,
			String linkerName, AOL defaultAOL) throws MojoExecutionException,
			MojoFailureException {
		File dir = getNarUnpackDirectory(unpackDirectory, file);

		boolean process = false;

		if (!unpackDirectory.exists()) {
			unpackDirectory.mkdirs();
			process = true;
		} else if (!dir.exists()) {
			process = true;
		} else if (file.lastModified() > dir.lastModified()) {
			try {
				FileUtils.deleteDirectory(dir);
			} catch (IOException e) {
				throw new MojoExecutionException("Could not delete directory: "
						+ dir, e);
			}

			process = true;
		}

		if (process) {
			unpackNarAndProcess(archiverManager, file, dir, os, linkerName,
					defaultAOL);
		}
	}

	@Override
	public File getNarUnpackDirectory(File baseUnpackDirectory, File narFile) {
		File dir = new File(baseUnpackDirectory, FileUtils.basename(
				narFile.getPath(), "." + NarConstants.NAR_EXTENSION));
		return dir;
	}

}
