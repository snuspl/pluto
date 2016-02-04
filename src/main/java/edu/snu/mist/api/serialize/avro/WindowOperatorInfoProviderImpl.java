/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.mist.api.serialize.avro;

import edu.snu.mist.api.WindowedStream;
import edu.snu.mist.api.window.*;
import edu.snu.mist.formats.avro.EmitPolicyTypeEnum;
import edu.snu.mist.formats.avro.SizePolicyTypeEnum;
import edu.snu.mist.formats.avro.WindowOperatorInfo;

import javax.inject.Inject;

/**
 * Default implementation for WindowOperatorInfoProvider interface.
 */
public final class WindowOperatorInfoProviderImpl implements WindowOperatorInfoProvider {

  @Inject
  private WindowOperatorInfoProviderImpl() {

  }

  @Override
  public WindowOperatorInfo getWindowOperatorInfo(final WindowedStream windowedStream) {
    final WindowOperatorInfo.Builder wOpInfoBuilder = WindowOperatorInfo.newBuilder();
    final WindowSizePolicy sizePolicy = windowedStream.getWindowSizePolicy();
    if (sizePolicy.getSizePolicyType() == WindowType.SizePolicy.TIME) {
      wOpInfoBuilder.setSizePolicyType(SizePolicyTypeEnum.TIME);
      wOpInfoBuilder.setSizePolicyInfo(((TimeSizePolicy) sizePolicy).getTimeDuration());
    } else {
      throw new IllegalStateException("WindowSizePolicy is illegal!");
    }
    final WindowEmitPolicy emitPolicy = windowedStream.getWindowEmitPolicy();
    if (emitPolicy.getEmitPolicyType() == WindowType.EmitPolicy.TIME) {
      wOpInfoBuilder.setEmitPolicyType(EmitPolicyTypeEnum.TIME);
      wOpInfoBuilder.setEmitPolicyInfo(((TimeEmitPolicy) emitPolicy).getTimeInterval());
    } else {
      throw new IllegalStateException("WindowEmitPolicy is illegal!");
    }
    return wOpInfoBuilder.build();
  }
}