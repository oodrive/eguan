package com.oodrive.nuage.nbdsrv.packet;

/*
 * #%L
 * Project eguan
 * %%
 * Copyright (C) 2012 - 2014 Oodrive
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;

import org.junit.Test;

public class TestPacket {

    private static String exportName = "testDevice";

    private final static byte[] init = { 0x4e, 0x42, 0x44, 0x4d, 0x41, 0x47, 0x49, 0x43, 0x49, 0x48, 0x41, 0x56, 0x45,
            0x4f, 0x50, 0x54, 0x00, 0x01 };

    private final static byte[] globalFlags = { 0x00, 0x00, 0x00, 0x01 };

    private final static byte[] optionList = { 0x49, 0x48, 0x41, 0x56, 0x45, 0x4f, 0x50, 0x54, 0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x00 };

    private final static byte[] replyListWithData = { 0x00, 0x03, -24, -119, 0x04, 0x55, 0x65, -87, 0x00, 0x00, 0x00,
            0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x00, 0x00, 0x0A, 0x74, 0x65, 0x73, 0x74, 0x44,
            0x65, 0x76, 0x69, 0x63, 0x65 };
    private final static byte[] replyListAck = { 0x00, 0x03, -24, -119, 0x04, 0x55, 0x65, -87, 0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00 };

    private final static byte[] replyList = { 0x00, 0x03, -24, -119, 0x04, 0x55, 0x65, -87, 0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x0e };
    private final static byte[] replyListData = { 0x00, 0x00, 0x00, 0x0A, 0x74, 0x65, 0x73, 0x74, 0x44, 0x65, 0x76,
            0x69, 0x63, 0x65 };

    private final static byte[] optionExportName = { 0x49, 0x48, 0x41, 0x56, 0x45, 0x4f, 0x50, 0x54, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00, 0x0A };
    private final static byte[] name = { 0x74, 0x65, 0x73, 0x74, 0x44, 0x65, 0x76, 0x69, 0x63, 0x65 };

    private final static byte[] exportFlags = { 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

    private final static byte[] dataPushingWrite = { 0x25, 0x60, -107, 0x13, 0x00, 0x00, 0x00, 0x01, 0x07, -80, 0x07,
            0x5b, -4, -117, -7, 0x23, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00 };

    private final static byte[] dataPushingRead = { 0x25, 0x60, -107, 0x13, 0x00, 0x00, 0x00, 0x00, 0x11, 0x36, 0x0e,
            0x75, -24, 0x59, 0x09, 0x2b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x00 };

    private final static byte[] dataPushingReplyWrite = { 0x67, 0x44, 0x66, -104, 0x00, 0x00, 0x00, 0x00, 0x07, -80,
            0x07, 0x5b, -4, -117, -7, 0x23 };

    private final static byte[] dataPushingReplyRead = { 0x67, 0x44, 0x66, -104, 0x00, 0x00, 0x00, 0x00, 0x11, 0x36,
            0x0e, 0x75, -24, 0x59, 0x09, 0x2b };

    @Test
    public void testInitPacketSerialize() {
        final InitPacket packet = new InitPacket(InitPacket.MAGIC_STR, InitPacket.MAGIC, 0x01);
        final ByteBuffer actual = InitPacket.serialize(packet);
        final ByteBuffer expected = ByteBuffer.wrap(init);

        assertEquals(expected, actual);

        InitPacket.release(actual);
    }

    @Test
    public void testInitPacketDeserialize() throws NbdException {
        final InitPacket expected = new InitPacket(InitPacket.MAGIC_STR, InitPacket.MAGIC, 0x01);
        final InitPacket actual = InitPacket.deserialize(ByteBuffer.wrap(init));

        assertEquals(expected.getMagicStr(), actual.getMagicStr());
        assertEquals(expected.getMagic(), actual.getMagic());
        assertEquals(expected.getGlobalFlags(), actual.getGlobalFlags());

    }

    @Test
    public void testGlobalFlagsSerialize() {
        final long flags = GlobalFlagsPacket.NBD_FLAG_FIXED_NEWSTYLE;
        final ByteBuffer actual = GlobalFlagsPacket.serialize(flags);
        final ByteBuffer expected = ByteBuffer.wrap(globalFlags);

        assertEquals(expected, actual);

        GlobalFlagsPacket.release(actual);
    }

    @Test
    public void testGlobalFlagsDeserialize() throws NbdException {
        final long expected = GlobalFlagsPacket.NBD_FLAG_FIXED_NEWSTYLE;
        final long actual = GlobalFlagsPacket.deserialize(ByteBuffer.wrap(globalFlags));

        assertEquals(expected, actual);
        assertTrue(GlobalFlagsPacket.checkNewStyle(actual));
    }

    @Test
    public void testOptionPacketSerialize() {
        // List
        {
            final OptionPacket packet = new OptionPacket(OptionPacket.MAGIC, OptionCmd.NBD_OPT_LIST, 0);
            final ByteBuffer[] actuals = OptionPacket.serialize(packet, null);
            final ByteBuffer expected = ByteBuffer.wrap(optionList);

            assertEquals(expected, actuals[0]);

            OptionPacket.release(actuals);
        }
        // Export Name
        {
            final OptionPacket packet = new OptionPacket(OptionPacket.MAGIC, OptionCmd.NBD_OPT_EXPORT_NAME,
                    exportName.length());
            final ByteBuffer[] actuals = OptionPacket.serialize(packet, exportName);
            final ByteBuffer[] expected = { ByteBuffer.wrap(optionExportName), ByteBuffer.wrap(name) };

            assertEquals(expected[0], actuals[0]);
            assertEquals(expected[1], actuals[1]);

            OptionPacket.release(actuals);
        }

    }

    @Test
    public void testOptionPacketDeserialize() throws NbdException {
        // List
        {
            final OptionPacket expected = new OptionPacket(OptionPacket.MAGIC, OptionCmd.NBD_OPT_LIST, 0);
            final ByteBuffer expectedHeader = ByteBuffer.wrap(optionList);
            final OptionPacket actual = OptionPacket.deserialize(expectedHeader);

            assertEquals(expected.getMagicNumber(), actual.getMagicNumber());
            assertEquals(expected.getOptionCode(), actual.getOptionCode());
            assertEquals(expected.getSize(), actual.getSize());
        }
        // Export Name
        {
            final OptionPacket expected = new OptionPacket(OptionPacket.MAGIC, OptionCmd.NBD_OPT_EXPORT_NAME,
                    exportName.length());

            // Deserialize header
            final ByteBuffer expectedHeader = ByteBuffer.wrap(optionExportName);
            final OptionPacket actual = OptionPacket.deserialize(expectedHeader);
            assertEquals(expected.getMagicNumber(), actual.getMagicNumber());
            assertEquals(expected.getOptionCode(), actual.getOptionCode());
            assertEquals(expected.getSize(), actual.getSize());

            // Deserialize data
            final ByteBuffer expectedData = ByteBuffer.wrap(name);
            assertEquals(exportName, OptionPacket.getData(expectedData));
        }

    }

    @Test
    public void testOptionReplyPacketSerialize() {

        // NBD_REP_SERVER
        {
            final OptionReplyPacket packet = new OptionReplyPacket(OptionReplyPacket.MAGIC, OptionCmd.NBD_OPT_LIST,
                    OptionReplyCmd.NBD_REP_SERVER);
            final String[] data = { exportName };
            // Serialize header + data
            final ByteBuffer[] actuals = OptionReplyPacket.serializeMultiple(packet, data);
            final ByteBuffer expectedHeader = ByteBuffer.wrap(replyListWithData);
            assertEquals(expectedHeader, actuals[0]);

            OptionReplyPacket.release(actuals);
        }
        // NBD_REP_ACK
        {
            final OptionReplyPacket packet = new OptionReplyPacket(OptionReplyPacket.MAGIC, OptionCmd.NBD_OPT_LIST,
                    OptionReplyCmd.NBD_REP_ACK);
            final ByteBuffer expected = ByteBuffer.wrap(replyListAck);
            // Serialize header + data (empty)
            final ByteBuffer actual = OptionReplyPacket.serialize(packet, "");
            assertEquals(expected, actual);

            OptionReplyPacket.release(actual);
        }
    }

    @Test
    public void testOptionReplyPacketDeserialize() {
        // NBD_REP_SERVER
        {
            final OptionReplyPacket expected = new OptionReplyPacket(OptionReplyPacket.MAGIC, OptionCmd.NBD_OPT_LIST,
                    OptionReplyCmd.NBD_REP_SERVER);
            // Set manually data size (is done in serialize)
            expected.setDataSize(exportName.length() + 4);

            // Deserialize header
            final ByteBuffer expectedHeader = ByteBuffer.wrap(replyList);
            final OptionReplyPacket actual = OptionReplyPacket.deserialize(expectedHeader);
            assertEquals(expected.getMagic(), actual.getMagic());
            assertEquals(expected.getOptionCmd(), actual.getOptionCmd());
            assertEquals(expected.getReplyCmd(), actual.getReplyCmd());
            assertEquals(expected.getDataSize(), actual.getDataSize());

            // Deserialize data
            final ByteBuffer expectedData = ByteBuffer.wrap(replyListData);
            assertEquals(exportName, OptionReplyPacket.getData(expectedData, expected)[0]);
        }
        // NBD_REP_ACK
        {
            final OptionReplyPacket expected = new OptionReplyPacket(OptionReplyPacket.MAGIC, OptionCmd.NBD_OPT_LIST,
                    OptionReplyCmd.NBD_REP_ACK);
            final ByteBuffer expectedHeader = ByteBuffer.wrap(replyListAck);
            final OptionReplyPacket actual = OptionReplyPacket.deserialize(expectedHeader);

            assertEquals(expected.getMagic(), actual.getMagic());
            assertEquals(expected.getOptionCmd(), actual.getOptionCmd());
            assertEquals(expected.getReplyCmd(), actual.getReplyCmd());
            assertEquals(expected.getDataSize(), actual.getDataSize());
        }
    }

    @Test
    public void testExportFlagsPacketSerialize() {
        final int flags = ExportFlagsPacket.NBD_FLAG_HAS_FLAGS;
        final ExportFlagsPacket packet = new ExportFlagsPacket(0x200000000L, flags);
        final ByteBuffer actual = ExportFlagsPacket.serialize(packet);
        final ByteBuffer expected = ByteBuffer.wrap(exportFlags);

        assertEquals(expected, actual);

        ExportFlagsPacket.release(actual);
    }

    @Test
    public void testExportFlagsPacketDeserialize() {
        final int flags = ExportFlagsPacket.NBD_FLAG_HAS_FLAGS;
        final ExportFlagsPacket expected = new ExportFlagsPacket(0x200000000L, flags);
        final ExportFlagsPacket actual = ExportFlagsPacket.deserialize(ByteBuffer.wrap(exportFlags));

        assertEquals(expected.getExportSize(), actual.getExportSize());
        assertEquals(expected.getExportFlags(), actual.getExportFlags());

    }

    @Test
    public void testDataPushingSerialize() {
        // read
        {
            final DataPushingPacket packet = new DataPushingPacket(DataPushingPacket.MAGIC,
                    DataPushingCmd.NBD_CMD_READ, 0x11360e75e859092bL, 0x00, 0x100000);
            final ByteBuffer actual = DataPushingPacket.serialize(packet);
            final ByteBuffer expected = ByteBuffer.wrap(dataPushingRead);

            assertEquals(expected, actual);
        }
        // write
        {
            final DataPushingPacket packet = new DataPushingPacket(DataPushingPacket.MAGIC,
                    DataPushingCmd.NBD_CMD_WRITE, 0x07b0075bfc8bf923L, 0x00, 0x100000);
            final ByteBuffer actual = DataPushingPacket.serialize(packet);
            final ByteBuffer expected = ByteBuffer.wrap(dataPushingWrite);

            assertEquals(expected, actual);
        }
    }

    @Test
    public void testDataPushingDeserialize() throws NbdException {
        // read
        {
            final DataPushingPacket expected = new DataPushingPacket(DataPushingPacket.MAGIC,
                    DataPushingCmd.NBD_CMD_READ, 0x11360e75e859092bL, 0x00, 0x100000);
            final DataPushingPacket actual = DataPushingPacket.deserialize(ByteBuffer.wrap(dataPushingRead));

            assertEquals(expected.getFrom(), actual.getFrom());
            assertEquals(expected.getHandle(), actual.getHandle());
            assertEquals(expected.getLen(), actual.getLen());
            assertEquals(expected.getMagic(), actual.getMagic());
            assertEquals(expected.getType(), actual.getType());
        }
        // write
        {
            final DataPushingPacket expected = new DataPushingPacket(DataPushingPacket.MAGIC,
                    DataPushingCmd.NBD_CMD_WRITE, 0x07b0075bfc8bf923L, 0x00, 0x100000);
            final DataPushingPacket actual = DataPushingPacket.deserialize(ByteBuffer.wrap(dataPushingWrite));

            assertEquals(expected.getFrom(), actual.getFrom());
            assertEquals(expected.getHandle(), actual.getHandle());
            assertEquals(expected.getLen(), actual.getLen());
            assertEquals(expected.getMagic(), actual.getMagic());
            assertEquals(expected.getType(), actual.getType());
        }
    }

    @Test
    public void testDataPushingReplySerialize() {
        // read
        {
            final DataPushingReplyPacket packet = new DataPushingReplyPacket(DataPushingReplyPacket.MAGIC,
                    DataPushingError.NBD_NO_ERROR, 0x11360e75e859092bL);
            final ByteBuffer actual = DataPushingReplyPacket.serialize(packet);
            final ByteBuffer expected = ByteBuffer.wrap(dataPushingReplyRead);

            assertEquals(expected, actual);
        }
        // write
        {
            final DataPushingReplyPacket packet = new DataPushingReplyPacket(DataPushingReplyPacket.MAGIC,
                    DataPushingError.NBD_NO_ERROR, 0x07b0075bfc8bf923L);
            final ByteBuffer actual = DataPushingReplyPacket.serialize(packet);
            final ByteBuffer expected = ByteBuffer.wrap(dataPushingReplyWrite);

            assertEquals(expected, actual);
        }
    }

    @Test
    public void testDataPushingReplyDeserialize() throws NbdException {
        // read
        {
            final DataPushingReplyPacket expected = new DataPushingReplyPacket(DataPushingReplyPacket.MAGIC,
                    DataPushingError.NBD_NO_ERROR, 0x11360e75e859092bL);
            final DataPushingReplyPacket actual = DataPushingReplyPacket.deserialize(ByteBuffer
                    .wrap(dataPushingReplyRead));

            assertEquals(expected.getError(), actual.getError());
            assertEquals(expected.getHandle(), actual.getHandle());
            assertEquals(expected.getMagic(), actual.getMagic());
        }
        // write
        {
            final DataPushingReplyPacket expected = new DataPushingReplyPacket(DataPushingReplyPacket.MAGIC,
                    DataPushingError.NBD_NO_ERROR, 0x07b0075bfc8bf923L);
            final DataPushingReplyPacket actual = DataPushingReplyPacket.deserialize(ByteBuffer
                    .wrap(dataPushingReplyWrite));

            assertEquals(expected.getError(), actual.getError());
            assertEquals(expected.getHandle(), actual.getHandle());
            assertEquals(expected.getMagic(), actual.getMagic());
        }
    }

}
