package com.oodrive.nuage.nbdsrv.packet;

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

import java.nio.ByteBuffer;

import org.junit.Test;

public class TestPacketException {

    @Test(expected = NbdException.class)
    public void testInitPacketBadMagicStr() throws NbdException {

        final byte[] init = { 0x00, 0x42, 0x44, 0x4d, 0x41, 0x47, 0x49, 0x43, 0x49, 0x48, 0x41, 0x56, 0x45, 0x4f, 0x50,
                0x54, 0x00, 0x01 };

        InitPacket.deserialize(ByteBuffer.wrap(init));
    }

    @Test(expected = NbdException.class)
    public void testInitPacketBadMagicNumber() throws NbdException {

        final byte[] init = { 0x4e, 0x42, 0x44, 0x4d, 0x41, 0x47, 0x49, 0x43, 0x00, 0x48, 0x41, 0x56, 0x45, 0x4f, 0x50,
                0x54, 0x00, 0x01 };

        InitPacket.deserialize(ByteBuffer.wrap(init));
    }

    @Test(expected = NbdException.class)
    public void testOptionPacketBadMagicNumber() throws NbdException {
        final byte[] optionList = { 0x00, 0x48, 0x41, 0x56, 0x45, 0x4f, 0x50, 0x54, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
                0x00, 0x00 };
        OptionPacket.deserialize(ByteBuffer.wrap(optionList));
    }

    @Test(expected = NbdException.class)
    public void testDataPushingReplyPacketBadMagicNumber() throws NbdException {
        final byte[] dataPushingReply = { 0x00, 0x44, 0x66, -104, 0x00, 0x00, 0x00, 0x00, 0x11, 0x36, 0x0e, 0x75, -24,
                0x59, 0x09, 0x2b };
        DataPushingReplyPacket.deserialize(ByteBuffer.wrap(dataPushingReply));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPacketSizeHigherThanAnInteger() {
        final OptionPacket packet = new OptionPacket(OptionPacket.MAGIC, OptionCmd.NBD_OPT_LIST, Integer.MAX_VALUE + 1);
        OptionPacket.serialize(packet, null);
    }
}
