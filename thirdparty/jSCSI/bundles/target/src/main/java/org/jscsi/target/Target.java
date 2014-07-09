package org.jscsi.target;

import org.jscsi.target.storage.IStorageModule;

/**
 * One Target exists per iSCSI named target. Holds onto the name and the {@link IStorageModule}
 * 
 * @author David L. Smith-Uchida
 * 
 *         jSCSI
 * 
 *         Copyright (C) 2009 iGeek, Inc. All Rights Reserved
 */
public class Target {
    private final String targetName;
    private final String targetAlias;
    private final IStorageModule storageModule;
    private final int hashcode;

    public Target(String targetName, String targetAlias, IStorageModule storageModule) {
        this.targetName = targetName;
        this.targetAlias = targetAlias;
        this.storageModule = storageModule;
        this.hashcode = targetName == null ? 0 : targetName.toLowerCase().hashCode();
    }

    public String getTargetName() {
        return targetName;
    }

    public String getTargetAlias() {
        return targetAlias;
    }

    public IStorageModule getStorageModule() {
        return storageModule;
    }

    @Override
    public int hashCode() {
        return hashcode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Target other = (Target)obj;
        if (targetName == null) {
            if (other.targetName != null)
                return false;
        } else if (!targetName.equalsIgnoreCase(other.targetName))
            return false;
        return true;
    }

}
