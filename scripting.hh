/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "config.h"
#include <string>
#include <map>

#include "ep.hh"
#include "locks.hh"
#include <memcached/extension.h>

extern "C" {
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
}

extern "C" {
    typedef ENGINE_ERROR_CODE (*RESPONSE_HANDLER_T)(const void *, int , const char *);
}

class ScriptCallback;

class ScriptGlobalRegistry {
public:

    ScriptGlobalRegistry() {}

    void registerGlobal(std::string name, std::string fun);

    void applyGlobals(lua_State *state);

private:
    Mutex mutex;
    std::map<std::string, std::string> globals;

    DISALLOW_COPY_AND_ASSIGN(ScriptGlobalRegistry);
};

class ScriptContext {
public:

    ScriptContext();

    ScriptContext(const ScriptContext& from);

    int eval(const char *script, const char **result, size_t *rlen);

    std::string load(const char *file);

    ~ScriptContext() {
        lua_close(luaState);
    }

    void initialize(EventuallyPersistentStore *s,
                    std::string initScript,
                    ScriptGlobalRegistry *globalRegistry);

private:
    friend class ScriptCallback;

    void initBaseLibs();

    lua_State* luaState;
    std::string initScript;
    EventuallyPersistentStore *store;
    ScriptGlobalRegistry *globalRegistry;

    void operator=(const ScriptContext&);
};
