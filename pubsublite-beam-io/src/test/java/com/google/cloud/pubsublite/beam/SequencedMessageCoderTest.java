// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.pubsublite.beam;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.pubsublite.Message;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.SequencedMessage;
import com.google.common.collect.ImmutableListMultimap;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SequencedMessageCoderTest {
  @Test
  public void roundTripMessages() throws IOException {
    SequencedMessage message1 =
        SequencedMessage.create(
            Message.builder()
                .setKey(ByteString.copyFromUtf8("abc"))
                .setData(ByteString.copyFromUtf8("def"))
                .setEventTime(Timestamps.EPOCH)
                .setAttributes(
                    ImmutableListMultimap.<String, ByteString>builder()
                        .put("xy", ByteString.copyFromUtf8("z"))
                        .build())
                .build(),
            Timestamps.EPOCH,
            Offset.create(10),
            1000);
    SequencedMessage message2 =
        SequencedMessage.create(message1.message(), Timestamps.EPOCH, Offset.create(88), 9854656);
    SequencedMessageCoder coder = new SequencedMessageCoder();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    coder.encode(message1, output);
    coder.encode(message2, output);
    ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
    assertThat(message1).isEqualTo(coder.decode(input));
    assertThat(message2).isEqualTo(coder.decode(input));
  }
}
