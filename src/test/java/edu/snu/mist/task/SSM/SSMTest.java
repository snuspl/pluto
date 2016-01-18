package edu.snu.mist.task.SSM;

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

import org.apache.reef.io.network.util.StringIdentifierFactory;
import org.apache.reef.tang.exceptions.InjectionException;
import org.junit.Assert;
import org.junit.Test;
import org.apache.reef.wake.Identifier;

public final class SSMTest {

    @Test
    public void dbTest() throws InjectionException {
        SSM ssm = new SSMImpl();
        ssm.open();

        Identifier id1 = new StringIdentifierFactory().getNewInstance("Operator1");
        Identifier id2 = new StringIdentifierFactory().getNewInstance("Operator2");

        Integer value1 = 1;
        Integer value2 = 2;
        Character value3= 'c';

        //Check if 'set' saves the right value
        ssm.set(id1, value1);
        Assert.assertEquals(value1, ssm.get(id1));

        //Check what happens when the value is replaced (on the same key)
        ssm.set(id1, value2);
        Assert.assertEquals(value2, ssm.get(id1));

        //Check if different types can be saved
        ssm.set(id2, value2);
        Assert.assertEquals(value2, ssm.get(id2));
        ssm.set(id2, value3);
        Assert.assertEquals(value3, ssm.get(id2));

        //Check if deletion works
        ssm.delete(id1);
        Assert.assertSame(null, ssm.get(id1));

        ssm.close();
    }


}
