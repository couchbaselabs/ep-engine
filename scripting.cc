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

    static int mc_add(lua_State *ls) {
        if (lua_gettop(ls) != 5) {
            lua_pushstring(ls, "mc.set takes five arguments: "
                           "vbucket, key, exp, flags, value");
            lua_error(ls);
            return 1;
        }

        int vb = luaL_checkint(ls, 1);
        const char *key = luaL_checkstring(ls, 2);
        rel_time_t exptime = static_cast<rel_time_t>(luaL_checkint(ls, 3));
        int flags = htonl(luaL_checkint(ls, 4));
        size_t val_len;
        const char *valptr = luaL_checklstring(ls, 5, &val_len);

        Item itm(key, strlen(key), flags, exptime, valptr, val_len, 0, -1, vb);

        ENGINE_ERROR_CODE rc = getStore(ls)->add(itm, NULL);
        if (rc != ENGINE_SUCCESS) {
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
        {"add", mc_add},
        {"del", mc_del},
        {NULL, NULL}
    };

    static int luaStringWriter(lua_State *,
                               const void* p,
                               size_t sz,
                               void* ud) {
        std::string *s = static_cast<std::string*>(ud);
        s->append(static_cast<const char*>(p), sz);
        return 0;
    }

}

ScriptContext::ScriptContext() : luaState(luaL_newstate()) {
    luaL_openlibs(luaState);
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

std::string ScriptContext::load(const char *path) {
    lua_settop(luaState, 0);

    if (luaL_loadfile(luaState, path) != 0) {
        size_t rlen;
        const char *m = lua_tolstring(luaState, 1, &rlen);
        throw std::string(m, rlen);
    }

    std::string rv;

    if (lua_dump(luaState, luaStringWriter, &rv)) {
        size_t rlen;
        const char *m = lua_tolstring(luaState, 1, &rlen);
        throw std::string(m, rlen);
    }

    return rv;
}


void ScriptContext::initialize(EventuallyPersistentStore *s,
                               GET_SERVER_API get_server_api,
                               std::string script) {
    store = s;
    serverApi = get_server_api();

    luaL_register(luaState, "mc", mc_funcs);

    lua_pushlightuserdata(luaState, (void *)&serverApiKey);
    lua_pushlightuserdata(luaState, serverApi);
    lua_settable(luaState, LUA_REGISTRYINDEX);

    lua_pushlightuserdata(luaState, (void *)&storeKey);
    lua_pushlightuserdata(luaState, store);
    lua_settable(luaState, LUA_REGISTRYINDEX);

    if (luaL_loadbuffer(luaState, script.data(), script.size(), "init") != 0) {
        size_t rlen;
        const char *m = lua_tolstring(luaState, 1, &rlen);
        throw std::string(m, rlen);
    }

    if (lua_pcall(luaState, 0, LUA_MULTRET, 0) != 0) {
        size_t rlen;
        const char *m = lua_tolstring(luaState, 1, &rlen);
        throw std::string(m, rlen);
    }

    // TODO Bring back the globals

    // Prevent any future global access
    lua_getfield(luaState, LUA_GLOBALSINDEX, "mc_post_init");
    if (lua_pcall(luaState, 0, 0, 0) != 0) {
        size_t rlen;
        const char *m = lua_tolstring(luaState, 1, &rlen);
        throw std::string(m, rlen);
    }
}
