package org.jscsi.target;

import javax.annotation.concurrent.Immutable;

/**
 * Statistics of a target.
 * 
 * @author llambert - OODRIVE
 */
@Immutable
public final class TargetStats {

    /** Name of the target */
    private final String name;
    /** Alias of the target */
    private final String alias;
    /** Number of connections */
    private final int connectionCount;
    /** Size in bytes */
    private final long size;
    /** Tells if the storage space is write protected. */
    private final boolean writeProtected;

    protected TargetStats(final String name, final String alias, final int connectionCount, final long size,
            final boolean writeProtected) {
        super();
        this.name = name;
        this.alias = alias == null ? "" : alias;
        this.connectionCount = connectionCount;
        this.size = size;
        this.writeProtected = writeProtected;
    }

    /**
     * Gets the name of the target
     * 
     * @return the target name
     */
    public final String getName() {
        return name;
    }

    /**
     * Gets the alias name of the target.
     * 
     * @return target alias
     */
    public final String getAlias() {
        return alias;
    }

    /**
     * Number of opened connections to the target.
     * 
     * @return number of opened connections.
     */
    public final int getConnectionCount() {
        return connectionCount;
    }

    /**
     * Size of the target in bytes.
     * 
     * @return size of the target.
     */
    public final long getSize() {
        return size;
    }

    /**
     * Tells if the target is write protected.
     * 
     * @return <code>true</code> if the target is write protected
     */
    public boolean isWriteProtected() {
        return writeProtected;
    }
}
