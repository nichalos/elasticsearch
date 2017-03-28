/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collections;

/**
 * Test that needsScores() is reported correctly depending on whether _score is used
 */
// TODO: can we test this better? this is a port of the ExpressionsTests method.
public class NeedsScoreTests extends ESSingleNodeTestCase {

    public void testNeedsScores() {
        IndexService index = createIndex("test", Settings.EMPTY, "type", "d", "type=double");

        PainlessScriptEngineService service = new PainlessScriptEngineService(Settings.EMPTY);
        SearchLookup lookup = new SearchLookup(index.mapperService(), index.fieldData(), null);

        Object compiled = service.compile(null, "1.2", Collections.emptyMap());
        SearchScript ss = service.search(new CompiledScript(ScriptType.INLINE, "randomName", "painless", compiled),
                                         lookup, Collections.<String, Object>emptyMap());
        assertFalse(ss.needsScores());

        compiled = service.compile(null, "doc['d'].value", Collections.emptyMap());
        ss = service.search(new CompiledScript(ScriptType.INLINE, "randomName", "painless", compiled),
                            lookup, Collections.<String, Object>emptyMap());
        assertFalse(ss.needsScores());

        compiled = service.compile(null, "1/_score", Collections.emptyMap());
        ss = service.search(new CompiledScript(ScriptType.INLINE, "randomName", "painless", compiled),
                            lookup, Collections.<String, Object>emptyMap());
        assertTrue(ss.needsScores());

        compiled = service.compile(null, "doc['d'].value * _score", Collections.emptyMap());
        ss = service.search(new CompiledScript(ScriptType.INLINE, "randomName", "painless", compiled),
                            lookup, Collections.<String, Object>emptyMap());
        assertTrue(ss.needsScores());
        service.close();
    }

}
