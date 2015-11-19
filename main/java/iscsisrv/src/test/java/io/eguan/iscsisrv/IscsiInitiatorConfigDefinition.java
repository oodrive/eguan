package io.eguan.iscsisrv;

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

import java.io.PrintStream;

final public class IscsiInitiatorConfigDefinition {

    public final static String INITIATOR_CONFIG_FILE_PREFIX = "initiator-tst-config";
    public final static String INITIATOR_CONFIG_FILE_SUFFIX = ".xml";

    public final static String INITIATOR_CONFIG_XML_VERSION = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>";
    public final static String INITIATOR_CONFIG_CONFIGURATION = "<configuration xmlns=\"http://www.jscsi.org/2006-09\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:schemaLocation=\"http://www.jscsi.org/2006-09 jscsi.xsd\">";

    public final static String INITIATOR_CONFIG_GLOBAL = "<global>";

    public final static String INITIATOR_CONFIG_AUTHMETHOD = "<AuthMethod>None</AuthMethod>";
    public final static String INITIATOR_CONFIG_DATADIGEST = "<DataDigest>None</DataDigest>";
    public final static String INITIATOR_CONFIG_DATAPDUINORDER = "<DataPDUInOrder>Yes</DataPDUInOrder>";
    public final static String INITIATOR_CONFIG_DATASEQINORDER = "<DataSequenceInOrder>Yes</DataSequenceInOrder>";
    public final static String INITIATOR_CONFIG_DEFAULTTIME2RETAIN = "<DefaultTime2Retain>20</DefaultTime2Retain>";
    public final static String INITIATOR_CONFIG_DEFAULTTIME2WAIT = "<DefaultTime2Wait>2</DefaultTime2Wait>";
    public final static String INITIATOR_CONFIG_ERRORRECOVERYLEVEL = "<ErrorRecoveryLevel>0</ErrorRecoveryLevel>";
    public final static String INITIATOR_CONFIG_FIRSTBURSTLENGTH = "<FirstBurstLength>65536</FirstBurstLength>";
    public final static String INITIATOR_CONFIG_HEADERDIGEST = "<HeaderDigest>None</HeaderDigest>";
    public final static String INITIATOR_CONFIG_IFMARKER = "<IFMarker>No</IFMarker>";
    public final static String INITIATOR_CONFIG_IMMEDIATEDATA = "<ImmediateData>Yes</ImmediateData>";
    public final static String INITIATOR_CONFIG_INITIALR2T = "<InitialR2T>Yes</InitialR2T>";
    public final static String INITIATOR_CONFIG_INITIATORALIAS = "<InitiatorAlias>TestInitiatorAlias</InitiatorAlias>";
    public final static String INITIATOR_CONFIG_INITIATORNAME = "<InitiatorName>TestInitiatorName</InitiatorName>";
    public final static String INITIATOR_CONFIG_MAXBURSTLENGTH = "<MaxBurstLength>262144</MaxBurstLength>";
    public final static String INITIATOR_CONFIG_MAXCONNECTIONS = "<MaxConnections>1</MaxConnections>";
    public final static String INITIATOR_CONFIG_MAXOUTSTANDINGR2T = "<MaxOutstandingR2T>1</MaxOutstandingR2T>";
    public final static String INITIATOR_CONFIG_MAXRCVDATASEGLENGTH = "<MaxRecvDataSegmentLength>8192</MaxRecvDataSegmentLength>";
    public final static String INITIATOR_CONFIG_OFMARKER = "<OFMarker>No</OFMarker>";
    public final static String INITIATOR_CONFIG_SESSIONTYPE = "<SessionType>Normal</SessionType>";

    public final static String INITIATOR_CONFIG_GLOBAL_END = "</global>";

    public final static String INITIATOR_CONFIG_TARGET = "<target id=\"";

    public final static String INITIATOR_CONFIG_ADDRESS = "\" address=\"localhost\" port=\"3260\">";
    public final static String INITIATOR_CONFIG_TARGET_IMMEDIATEDATA = "<ImmediateData>Yes</ImmediateData>";
    public final static String INITIATOR_CONFIG_TARGET_INITIATORNAME = "<InitiatorName>TestingInitiator</InitiatorName>";
    public final static String INITIATOR_CONFIG_TARGET_TARGETNAME = "<TargetName>";

    public final static String INITIATOR_CONFIG_TARGET_TARGETNAME_END = "</TargetName>";
    public final static String INITIATOR_CONFIG_TARGET_END = "</target>";
    public final static String INITIATOR_CONFIG_CONFIGURATION_END = "</configuration>";

    static final void configWriteBegin(final PrintStream config) {
        config.println(INITIATOR_CONFIG_XML_VERSION);
        config.println(INITIATOR_CONFIG_CONFIGURATION);
        config.println(INITIATOR_CONFIG_GLOBAL);
        config.println(INITIATOR_CONFIG_AUTHMETHOD);
        config.println(INITIATOR_CONFIG_DATADIGEST);
        config.println(INITIATOR_CONFIG_DATAPDUINORDER);
        config.println(INITIATOR_CONFIG_DATASEQINORDER);
        config.println(INITIATOR_CONFIG_DEFAULTTIME2RETAIN);
        config.println(INITIATOR_CONFIG_DEFAULTTIME2WAIT);
        config.println(INITIATOR_CONFIG_ERRORRECOVERYLEVEL);
        config.println(INITIATOR_CONFIG_FIRSTBURSTLENGTH);
        config.println(INITIATOR_CONFIG_HEADERDIGEST);
        config.println(INITIATOR_CONFIG_IFMARKER);
        config.println(INITIATOR_CONFIG_IMMEDIATEDATA);
        config.println(INITIATOR_CONFIG_INITIALR2T);
        config.println(INITIATOR_CONFIG_INITIATORALIAS);
        config.println(INITIATOR_CONFIG_INITIATORNAME);
        config.println(INITIATOR_CONFIG_MAXBURSTLENGTH);
        config.println(INITIATOR_CONFIG_MAXCONNECTIONS);
        config.println(INITIATOR_CONFIG_MAXOUTSTANDINGR2T);
        config.println(INITIATOR_CONFIG_MAXRCVDATASEGLENGTH);
        config.println(INITIATOR_CONFIG_OFMARKER);
        config.println(INITIATOR_CONFIG_SESSIONTYPE);
        config.println(INITIATOR_CONFIG_GLOBAL_END);
    }

    static final void configWriteUpToTargetId(final PrintStream config) {
        config.print(INITIATOR_CONFIG_TARGET);
    }

    static final void configWriteUpToTargetName(final PrintStream config) {
        config.println(INITIATOR_CONFIG_ADDRESS);
        config.println(INITIATOR_CONFIG_TARGET_IMMEDIATEDATA);
        config.println(INITIATOR_CONFIG_TARGET_INITIATORNAME);
        config.print(INITIATOR_CONFIG_TARGET_TARGETNAME);
    }

    static final void configWriteAfterTargetName(final PrintStream config) {
        config.println(INITIATOR_CONFIG_TARGET_TARGETNAME_END);
        config.println(INITIATOR_CONFIG_TARGET_END);
    }

    static final void configWriteEnd(final PrintStream config) {
        config.println(INITIATOR_CONFIG_CONFIGURATION_END);
    }
}
