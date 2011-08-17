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
    static const char serverApiKey = 'S';
    static const char storeKey =     'E';

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

    static EventuallyPersistentStore *getStore(lua_State *ls) {
        lua_pushlightuserdata(ls, (void*)&storeKey);
        lua_gettable(ls, LUA_REGISTRYINDEX);
        assert(lua_isuserdata(ls, -1));

        EventuallyPersistentStore *store =
            static_cast<EventuallyPersistentStore*>(lua_touserdata(ls, -1));
        return store;
    }

    static int mc_get(lua_State *ls) {
        if (lua_gettop(ls) != 2) {
            lua_pushstring(ls, "mc.get takes two arguments: vbucket, key");
            lua_error(ls);
            return 1;
        }

        int vb = luaL_checkint(ls, 1);
        const char *key = luaL_checkstring(ls, 2);

        GetValue gv(getStore(ls)->get(key, vb, NULL, false));

        if (gv.getStatus() != ENGINE_SUCCESS) {
            lua_pushstring(ls, "error retrieving stuff");
            lua_error(ls);
            return 1;
        } else {
            StoredValue *v = gv.getStoredValue();
            lua_pushinteger(ls, ntohl(v->getFlags()));
            lua_pushinteger(ls, v->getCas());
            value_t val = v->getValue();
            lua_pushlstring(ls, val->getData(), val->length());
            return 3;
        }
    }

    static int mc_set(lua_State *ls) {
        if (lua_gettop(ls) < 5) {
            lua_pushstring(ls, "mc.set takes five or six arguments: "
                           "vbucket, key, exp, flags, value, [cas]");
            lua_error(ls);
            return 1;
        }

        int vb = luaL_checkint(ls, 1);
        const char *key = luaL_checkstring(ls, 2);
        rel_time_t exptime = static_cast<rel_time_t>(luaL_checkint(ls, 3));
        int flags = htonl(luaL_checkint(ls, 4));
        size_t val_len;
        const char *valptr = luaL_checklstring(ls, 5, &val_len);
        uint64_t cas(lua_gettop(ls) > 5 ? luaL_checkinteger(ls, 6) : 0);

        Item itm(key, strlen(key), flags, exptime, valptr, val_len, cas, -1, vb);

        if (getStore(ls)->set(itm, NULL) != ENGINE_SUCCESS) {
            lua_pushstring(ls, "storage error");
            lua_error(ls);
            return 1;
        }

        lua_pushinteger(ls, itm.getCas());

        return 1;
    }

    static int mc_del(lua_State *ls) {
        if (lua_gettop(ls) != 2) {
            lua_pushstring(ls, "mc.del takes two arguments: vbucket, key");
            lua_error(ls);
            return 1;
        }

        int vb = luaL_checkint(ls, 1);
        const char *key = luaL_checkstring(ls, 2);

        if (getStore(ls)->deleteItem(key, 0, 0, vb,
                                     NULL, false, false) != ENGINE_SUCCESS) {
            lua_pushstring(ls, "error deleting");
            lua_error(ls);
            return 1;
        }
        return 0;
    }

    static const luaL_Reg mc_funcs[] = {
        {"get", mc_get},
        {"set", mc_set},
        {"del", mc_del},
        {NULL, NULL}
    };
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

int ScriptContext::eval(const char *script, const char **result, size_t *rlen) {
    lua_settop(luaState, 0);

    if (luaL_loadstring(luaState, script) != 0) {
        const char *m = lua_tolstring(luaState, 1, rlen);
        *result = m;
        return 1;
    }

    if (lua_pcall(luaState, 0, LUA_MULTRET, 0) != 0) {
        const char *m = lua_tolstring(luaState, 1, rlen);
        *result = m;
        return 1;
    }

    if (lua_gettop(luaState) >= 1 && lua_isstring(luaState, 1)) {
        const char *m = lua_tolstring(luaState, 1, rlen);
        *result = m;
    }

    return 0;
}

void ScriptContext::initialize(EventuallyPersistentStore *s,
                               GET_SERVER_API get_server_api) {
    store = s;
    serverApi = get_server_api();

    luaL_register(luaState, "mc", mc_funcs);

    lua_pushlightuserdata(luaState, (void *)&serverApiKey);
    lua_pushlightuserdata(luaState, serverApi);
    lua_settable(luaState, LUA_REGISTRYINDEX);

    lua_pushlightuserdata(luaState, (void *)&storeKey);
    lua_pushlightuserdata(luaState, store);
    lua_settable(luaState, LUA_REGISTRYINDEX);

    std::cerr << "Setting up script extensions" << std::endl;
    lua_getfield(luaState, LUA_GLOBALSINDEX, "setup_extensions");
    lua_pushcfunction(luaState, register_extension);
    if (lua_pcall(luaState, 1, 0, 0)) {
        std::cerr << "Error initializing registration: "
                  << lua_tostring(luaState, -1) << std::endl;
    }
}
