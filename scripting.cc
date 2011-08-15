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

#include "scripting.hh"

extern "C" {

    // References to global data
    const char serverApiKey = 'S';

    static ScriptAsciiExtension* script_ascii_cast(const void *cmd_cookie) {
        return static_cast<ScriptAsciiExtension*>(const_cast<void*>(cmd_cookie));
    }

    static const char *script_get_name(const void *cmd_cookie) {
        ScriptAsciiExtension *ext = script_ascii_cast(cmd_cookie);
        return ext->getName();
    }

    ENGINE_ERROR_CODE script_execute(const void *cmd_cookie, const void *cookie,
                                     int argc, token_t *argv,
                                     RESPONSE_HANDLER_T response_handler) {
        ScriptAsciiExtension *ext = script_ascii_cast(cmd_cookie);
        return ext->doExecute(cookie, argc, argv, response_handler);
    }

    bool script_accept(const void *cmd_cookie,
                       void *cookie,
                       int argc,
                       token_t *argv,
                       size_t *ndata,
                       char **ptr) {
        ScriptAsciiExtension *ext = script_ascii_cast(cmd_cookie);
        return ext->doAccept(cookie, argc, argv, ndata, ptr);
    }

    void script_abort(const void *cmd_cookie, const void *cookie) {
        ScriptAsciiExtension *ext = script_ascii_cast(cmd_cookie);
        ext->doAbort(cookie);
    }

    int register_extension(lua_State *luaState) {
        lua_pushlightuserdata(luaState, (void *)&serverApiKey);
        lua_gettable(luaState, LUA_REGISTRYINDEX);
        void *sapiP = lua_touserdata(luaState, -1);

        SERVER_HANDLE_V1 *serverApi = static_cast<SERVER_HANDLE_V1*>(sapiP);

        const char * name = lua_tostring(luaState, 1);

        lua_getfield(luaState, LUA_GLOBALSINDEX, "ascii_extensions");
        lua_settable(luaState, -1);

        std::cout << "Registering " << name << std::endl;

        ScriptAsciiExtension *ext = new ScriptAsciiExtension(luaState, name);
        serverApi->extension->register_extension(EXTENSION_ASCII_PROTOCOL, ext);
        return 0;
    }
}

ScriptAsciiExtension::ScriptAsciiExtension(lua_State *st, const char *n)
    : state(st), name(n) {
    get_name = script_get_name;
    accept = script_accept;
    execute = script_execute;
    abort = script_abort;
    cookie = this;
}

ENGINE_ERROR_CODE ScriptAsciiExtension::doExecute(const void *c,
                                                  int argc, token_t *argv,
                                                  RESPONSE_HANDLER_T response_handler) {
    (void)c;
    (void)argc;
    (void)argv;
    (void)response_handler;
    std::cout << "Handling " << argv[0].value << std::endl;
    return ENGINE_SUCCESS;
}

bool ScriptAsciiExtension::doAccept(void *c, int argc, token_t *argv, size_t *ndata,
                                    char **ptr) {
    (void)c;
    (void)argc;
    (void)argv;
    (void)ndata;
    (void)ptr;
    return strncmp(argv[0].value, name, argv[0].length) == 0;
}

void ScriptAsciiExtension::doAbort(const void *c) {
    (void)c;
}

std::string ScriptContext::eval(const char *script) {
    lua_settop(luaState, 0);

    if (luaL_loadstring(luaState, script) != 0) {
        std::cerr << "Error compiling this rubbish:\n" << script << std::endl;
        return "Compilation error";
    }

    if (lua_pcall(luaState, 0, LUA_MULTRET, 0) != 0) {
        std::cerr << "Error running the script.\n";
    }

    std::string rv;
    if (lua_gettop(luaState) >= 1 && lua_isstring(luaState, 1)) {
        size_t slen;
        const char *s = lua_tolstring(luaState, 1, &slen);
        if (s) {
            rv.assign(s, slen);
        }
    }

    return rv;
}

void ScriptContext::initialize(EventuallyPersistentStore *s,
                               GET_SERVER_API get_server_api) {
    store = s;
    serverApi = get_server_api();

    lua_pushlightuserdata(luaState, (void *)&serverApiKey);
    lua_pushlightuserdata(luaState, serverApi);
    lua_settable(luaState, LUA_REGISTRYINDEX);

    std::cerr << "Setting up script extensions" << std::endl;
    lua_getfield(luaState, LUA_GLOBALSINDEX, "setup_extensions");
    lua_pushcfunction(luaState, register_extension);
    if (lua_pcall(luaState, 1, 0, 0)) {
        std::cerr << "Error initializing registration: "
                  << lua_tostring(luaState, -1) << std::endl;
    }
}
