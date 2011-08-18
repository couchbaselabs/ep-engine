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

#include "common.hh"
#include "scripting.hh"
#include "dispatcher.hh"
#include "vbucket.hh"
#include "ep.hh"

class ScriptContext;

/**
 * Dispatcher job to run a script.
 */
class ScriptCallback : public DispatcherCallback {
public:
    ScriptCallback(ScriptContext *c, const std::string n, const std::string f)
        : ctx(c), name(n), fun(f) {
        assert(ctx);
        assert(name.size() > 0);
        assert(fun.size() > 0);
    }

    ~ScriptCallback() {
        delete ctx;
    }

    bool callback(Dispatcher &, TaskId);

    std::string description() {
        std::stringstream ss;
        ss << "Running lua script named:  " << name;
        return ss.str();
    }

private:
    ScriptContext     *ctx;
    const std::string  name;
    const std::string  fun;
};

class ScriptVBucketVisitor : public VBucketVisitor {
public:
    ScriptVBucketVisitor(ScriptContext *c,
                         const std::string inf,
                         const std::string vbf,
                         const std::string svf,
                         const std::string tf)
        : ctx(c), initfun(inf), vbfun(vbf), svfun(svf), teardownfun(tf) {
        assert(ctx);
        assert(vbfun.size() > 0);
        assert(svfun.size() > 0);

        maybeInit();
    }

    ~ScriptVBucketVisitor() {
        // Final invocation to let the script know it can clean up now.
        if (teardownfun.size() > 0) {
            lua_State *ls(ctx->getState());

            luaL_loadbuffer(ls, teardownfun.data(), teardownfun.size(),
                            "teardownfun");

            if (lua_pcall(ls, 0, 0, 0) != 0) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "vbucket script teardown error:  %s\n",
                                 lua_tostring(ls, 1));
            }
        }

        delete ctx;
    }

    void maybeInit() {
        if (initfun.size() > 0) {
            lua_State *ls(ctx->getState());

            luaL_loadbuffer(ls, initfun.data(), initfun.size(), "initfun");

            if (lua_pcall(ls, 0, 0, 0) != 0) {
                getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                                 "vbucket script init error:  %s\n",
                                 lua_tostring(ls, 1));
            }
        }
    }

    bool visitBucket(RCPtr<VBucket> &vb) {
        lua_State *ls(ctx->getState());

        luaL_loadbuffer(ls, vbfun.data(), vbfun.size(), "vbfun");
        lua_pushinteger(ls, vb->getId());
        lua_pushstring(ls, VBucket::toString(vb->getState()));

        if (lua_pcall(ls, 2, 1, 0) != 0) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "vbucket script execution error:  %s\n",
                             lua_tostring(ls, 1));
            return false;
        }

        vbucket = vb->getId();
        return lua_toboolean(ls, 1) == 1;
    }

    void visit(StoredValue *v) {
        lua_State *ls(ctx->getState());

        luaL_loadbuffer(ls, svfun.data(), svfun.size(), "vbfun");
        lua_pushinteger(ls, vbucket);
        lua_pushlstring(ls, v->getKeyBytes(), v->getKeyLen());
        lua_pushinteger(ls, v->getExptime());
        lua_pushinteger(ls, ntohl(v->getFlags()));
        lua_pushlightuserdata(ls, v);

        if (lua_pcall(ls, 5, 0, 0) != 0) {
            getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                             "data script execution error:  %s\n",
                             lua_tostring(ls, 1));
        }
    }

private:
    ScriptContext     *ctx;
    const std::string  initfun;
    const std::string  vbfun;
    const std::string  svfun;
    const std::string  teardownfun;
    uint16_t           vbucket;

    DISALLOW_COPY_AND_ASSIGN(ScriptVBucketVisitor);
};

class ScriptVBucketVisitorCallback : public DispatcherCallback {
public:

    ScriptVBucketVisitorCallback(EventuallyPersistentStore *s,
                                 ScriptVBucketVisitor *v) : store(s), vbv(v) {
        assert(s);
        assert(vbv);
    }

    ~ScriptVBucketVisitorCallback() {
        delete vbv;
    }

    bool callback(Dispatcher &, TaskId) {
        store->visit(*vbv);
        return false;
    }

    std::string description() {
        std::stringstream ss;
        ss << "Running a scripted vbucket visitor";
        return ss.str();
    }

private:
    EventuallyPersistentStore *store;
    ScriptVBucketVisitor *vbv;

    DISALLOW_COPY_AND_ASSIGN(ScriptVBucketVisitorCallback);
};

extern "C" {

    // References to global data
    static const char storeKey =     'E';
    static const char globalsKey =   'G';
    static const char contextKey =   'S';

    // Specific to dispatcher invocations
    static const char dispatcherKey = 'D';
    static const char taskKey =       'T';

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

    static int register_global(lua_State *ls) {
        if (lua_gettop(ls) != 2) {
            lua_pushstring(ls, "register_global takes two arguments:  name, function");
            lua_error(ls);
            return 1;
        }

        const char *name = luaL_checkstring(ls, 1);
        if (!lua_isfunction(ls, 2)) {
            lua_pushstring(ls, "Second argument must be a function.");
            lua_error(ls);
            return 1;
        }

        std::string fun;

        if (lua_dump(ls, luaStringWriter, &fun)) {
            size_t rlen;
            const char *m = lua_tolstring(ls, 1, &rlen);
            throw std::string(m, rlen);
        }

        lua_pushlightuserdata(ls, (void*)&globalsKey);
        lua_gettable(ls, LUA_REGISTRYINDEX);
        assert(lua_isuserdata(ls, -1));

        ScriptGlobalRegistry *globals =
            static_cast<ScriptGlobalRegistry*>(lua_touserdata(ls, -1));
        globals->registerGlobal(name, fun);

        return 0;
    }

    static const luaL_Reg core_funcs[] = {
        {"register_global", register_global},
        {NULL, NULL}
    };

    static int dispatcher_schedule(lua_State *ls) {
        if (lua_gettop(ls) < 3) {
            lua_pushstring(ls, "schedule takes three arguments: "
                           "name, function, delay");
            lua_error(ls);
            return 1;
        }

        const char *name = luaL_checkstring(ls, 1);
        if (!lua_isfunction(ls, 2)) {
            char errmsg[256];
            snprintf(errmsg, sizeof(errmsg) -1,
                     "Second argument must be a function, not a %s",
                     lua_typename(ls, lua_type(ls, 2)));
            lua_pushstring(ls, errmsg);
            lua_error(ls);
            return 1;
        }
        double delay = luaL_checknumber(ls, 3);

        std::string initFun;
        if (lua_gettop(ls) == 4) {
            if (lua_dump(ls, luaStringWriter, &initFun)) {
                lua_pushstring(ls, "fourth argument must be a function.");
                lua_error(ls);
                return 1;
            }
            lua_pop(ls, 1);
        }

        // Now grab the actual function
        lua_pop(ls, 1);
        std::string fun;

        if (lua_dump(ls, luaStringWriter, &fun)) {
            lua_pushstring(ls, "Second argument must be a function.");
            lua_error(ls);
            return 1;
        }

        lua_pushlightuserdata(ls, (void*)&contextKey);
        lua_gettable(ls, LUA_REGISTRYINDEX);
        assert(lua_isuserdata(ls, -1));

        ScriptContext *ctx = static_cast<ScriptContext*>(lua_touserdata(ls, -1));

        ScriptContext *newCtx = new ScriptContext(*ctx);

        // Stack adjusted has an extra function
        lua_getglobal(newCtx->getState(), "mc_init_dispatcher_job");
        if (initFun.size() > 0) {
            if (luaL_loadbuffer(newCtx->getState(), initFun.data(), initFun.size(),
                                "init") != 0) {
                lua_pushstring(ls, lua_tostring(newCtx->getState(), 1));
                lua_error(ls);
                return 1;
            }

        } else {
            lua_pushnil(newCtx->getState());
        }

        if (lua_pcall(newCtx->getState(), 1, 0, 0) != 0) {
            lua_pushstring(ls, lua_tostring(newCtx->getState(), 1));
            lua_error(ls);
            return 1;
        }

        shared_ptr<ScriptCallback> scb(new ScriptCallback(newCtx, name, fun));
        getStore(ls)->getNonIODispatcher()->schedule(scb, NULL,
                                                     Priority::ScriptPriority,
                                                     delay);
        return 0;
    }

    static int dispatcher_snooze(lua_State *ls) {
        double delay = luaL_checknumber(ls, 1);

        lua_pushlightuserdata(ls, (void*)&dispatcherKey);
        lua_gettable(ls, LUA_REGISTRYINDEX);
        if (!lua_isuserdata(ls, -1)) {
            lua_pushstring(ls, "You can only snooze when executing within a dispatcher.");
            lua_error(ls);
            return 1;
        }

        Dispatcher *d = static_cast<Dispatcher*>(lua_touserdata(ls, -1));

        lua_pushlightuserdata(ls, (void*)&taskKey);
        lua_gettable(ls, LUA_REGISTRYINDEX);
        assert(lua_isuserdata(ls, -1));

        TaskId *t = static_cast<TaskId*>(lua_touserdata(ls, -1));

        d->snooze(*t, delay);
        return 0;
    }

    static const luaL_Reg dispatcher_funcs[] = {
        {"schedule", dispatcher_schedule},
        {"snooze", dispatcher_snooze},
        {NULL, NULL}
    };

    static int data_iterate(lua_State *ls) {
        if (lua_gettop(ls) != 4) {
            lua_pushstring(ls, "iterate takes four function arguments: "
                           "initfun(), f(vb,state) => true, "
                           "f(k, exp, flags, v), teardownfun()");
            lua_error(ls);
            return 1;
        }

        std::string teardownfun;
        if (lua_dump(ls, luaStringWriter, &teardownfun)) {
            lua_pushstring(ls, "fourth argument must be a function.");
            lua_error(ls);
            return 1;
        }
        lua_pop(ls, 1);

        std::string datafun;
        if (lua_dump(ls, luaStringWriter, &datafun)) {
            lua_pushstring(ls, "third argument must be a function.");
            lua_error(ls);
            return 1;
        }
        lua_pop(ls, 1);

        std::string vbfun;
        if (lua_dump(ls, luaStringWriter, &vbfun)) {
            lua_pushstring(ls, "second argument must be a function.");
            lua_error(ls);
            return 1;
        }
        lua_pop(ls, 1);

        std::string initfun;
        if (lua_dump(ls, luaStringWriter, &initfun)) {
            lua_pushstring(ls, "first argument must be a function.");
            lua_error(ls);
            return 1;
        }
        lua_pop(ls, 1);

        EventuallyPersistentStore *store = getStore(ls);
        lua_pushlightuserdata(ls, (void*)&contextKey);
        lua_gettable(ls, LUA_REGISTRYINDEX);
        assert(lua_isuserdata(ls, -1));

        ScriptContext *ctx = static_cast<ScriptContext*>(lua_touserdata(ls, -1));
        ScriptContext *newCtx = new ScriptContext(*ctx);
        ScriptVBucketVisitor *v = new ScriptVBucketVisitor(newCtx,
                                                           initfun,
                                                           vbfun, datafun,
                                                           teardownfun);
        shared_ptr<ScriptVBucketVisitorCallback> scb(new ScriptVBucketVisitorCallback(store, v));
        getStore(ls)->getNonIODispatcher()->schedule(scb, NULL,
                                                     Priority::ScriptPriority,
                                                     0);
        return 0;
    }

    static const luaL_Reg data_funcs[] = {
        {"iterate", data_iterate},
        {NULL, NULL}
    };
}

ScriptContext::ScriptContext() : luaState(luaL_newstate()) {
    initBaseLibs();
}

ScriptContext::ScriptContext(const ScriptContext& from) : luaState(luaL_newstate()) {
    initBaseLibs();
    initialize(getStore(from.luaState),
               from.initScript,
               from.globalRegistry);
}

void ScriptContext::initBaseLibs() {
    static const luaL_Reg stdlibs[] = {
        {"", luaopen_base},
        {LUA_LOADLIBNAME, luaopen_package},
        {LUA_TABLIBNAME, luaopen_table},
        // {LUA_IOLIBNAME, luaopen_io},
        // {LUA_OSLIBNAME, luaopen_os},
        {LUA_STRLIBNAME, luaopen_string},
        {LUA_MATHLIBNAME, luaopen_math},
        // {LUA_DBLIBNAME, luaopen_debug},
        {NULL, NULL}
    };

    const luaL_Reg *lib = stdlibs;
    for (; lib->func; lib++) {
        lua_pushcfunction(luaState, lib->func);
        lua_pushstring(luaState, lib->name);
        lua_call(luaState, 1, 0);
    }
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
                               std::string init,
                               ScriptGlobalRegistry *reg) {
    store = s;
    initScript = init;
    globalRegistry = reg;

    luaL_register(luaState, "mc", mc_funcs);
    luaL_register(luaState, "ep_core", core_funcs);
    luaL_register(luaState, "dispatcher", dispatcher_funcs);
    luaL_register(luaState, "data", data_funcs);

    lua_pushlightuserdata(luaState, (void *)&contextKey);
    lua_pushlightuserdata(luaState, this);
    lua_settable(luaState, LUA_REGISTRYINDEX);

    lua_pushlightuserdata(luaState, (void *)&storeKey);
    lua_pushlightuserdata(luaState, store);
    lua_settable(luaState, LUA_REGISTRYINDEX);

    lua_pushlightuserdata(luaState, (void *)&globalsKey);
    lua_pushlightuserdata(luaState, globalRegistry);
    lua_settable(luaState, LUA_REGISTRYINDEX);

    if (luaL_loadbuffer(luaState, initScript.data(), initScript.size(),
                        "init") != 0) {
        size_t rlen;
        const char *m = lua_tolstring(luaState, 1, &rlen);
        throw std::string(m, rlen);
    }

    if (lua_pcall(luaState, 0, LUA_MULTRET, 0) != 0) {
        size_t rlen;
        const char *m = lua_tolstring(luaState, 1, &rlen);
        throw std::string(m, rlen);
    }

    globalRegistry->applyGlobals(luaState);

    // Prevent any future global access
    lua_getfield(luaState, LUA_GLOBALSINDEX, "mc_post_init");
    if (lua_pcall(luaState, 0, 0, 0) != 0) {
        size_t rlen;
        const char *m = lua_tolstring(luaState, 1, &rlen);
        throw std::string(m, rlen);
    }
}

void ScriptGlobalRegistry::registerGlobal(std::string name, std::string fun) {
    LockHolder lh(mutex);
    globals[name] = fun;
}

void ScriptGlobalRegistry::applyGlobals(lua_State *ls) {
    LockHolder lh(mutex);
    std::map<std::string, std::string>::const_iterator iter;
    for (iter = globals.begin(); iter != globals.end(); ++iter) {
        if (luaL_loadbuffer(ls, iter->second.data(), iter->second.size(), "globals") != 0) {
            size_t rlen;
            const char *m = lua_tolstring(ls, 1, &rlen);
            throw std::string(m, rlen);
        }

        lua_setglobal(ls, iter->first.c_str());
    }
}

bool ScriptCallback::callback(Dispatcher &d, TaskId t) {
    lua_State *luaState(ctx->luaState);
    lua_settop(luaState, 0);

    // Record our task info.
    lua_pushlightuserdata(luaState, (void *)&dispatcherKey);
    lua_pushlightuserdata(luaState, &d);
    lua_settable(luaState, LUA_REGISTRYINDEX);

    lua_pushlightuserdata(luaState, (void *)&taskKey);
    lua_pushlightuserdata(luaState, &t);
    lua_settable(luaState, LUA_REGISTRYINDEX);

    if (luaL_loadbuffer(luaState, fun.data(), fun.size(), name.c_str()) != 0) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Background script ``%s'' parsing error:  %s\n",
                         name.c_str(), lua_tostring(luaState, 1));
        return false;
    }

    lua_getglobal(ctx->luaState, "dispatcher_state");

    if (lua_pcall(luaState, 1, 1, 0) != 0) {
        getLogger()->log(EXTENSION_LOG_WARNING, NULL,
                         "Background script ``%s'' execution error:  %s\n",
                         name.c_str(), lua_tostring(luaState, 1));
        return false;
    }

    if (lua_gettop(luaState) >= 1 && lua_isboolean(luaState, 1)) {
        return static_cast<bool>(lua_toboolean(luaState, 1));
    }

    return false;
}
