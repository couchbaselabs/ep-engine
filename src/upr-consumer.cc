/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2013 Couchbase, Inc
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

#include "ep_engine.h"

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprAddStream(const void* cookie,
                                                           uint32_t opaque,
                                                           uint16_t vbucket,
                                                           uint32_t flags)
{
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprCloseStream(const void* cookie,
                                                             uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprStreamEnd(const void* cookie,
                                                           uint32_t opaque,
                                                           uint16_t vbucket,
                                                           uint32_t flags)
{
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprSnapshotMarker(const void* cookie,
                                                                uint32_t opaque,
                                                                uint16_t vbucket)
{
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprMutation(const void* cookie,
                                                          uint32_t opaque,
                                                          const void *key,
                                                          uint16_t nkey,
                                                          const void *value,
                                                          uint32_t nvalue,
                                                          uint64_t cas,
                                                          uint16_t vbucket,
                                                          uint32_t flags,
                                                          uint8_t datatype,
                                                          uint64_t bySeqno,
                                                          uint64_t revSeqno,
                                                          uint32_t expiration,
                                                          uint32_t lockTime)
{
    void *specific = getEngineSpecific(cookie);
    UprConsumer *consumer = NULL;
    if (specific == NULL) {
        // Create a new tap consumer...
        consumer = uprConnMap_->newConsumer(cookie);
        if (consumer == NULL) {
            LOG(EXTENSION_LOG_WARNING, "Failed to create new upr consumer. "
                "Force disconnect\n");
            return ENGINE_DISCONNECT;
        }
        storeEngineSpecific(cookie, consumer);
    }
    else {
        consumer = reinterpret_cast<UprConsumer *>(specific);
    }

    storeEngineSpecific(cookie, consumer);

    std::string k(static_cast<const char*>(key), nkey);
    ENGINE_ERROR_CODE ret = ConnHandlerMutate(consumer, k, cookie, flags, expiration, cas,
                                              revSeqno, vbucket, true, value, nvalue);
    return ret;
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprDeletion(const void* cookie,
                                                          uint32_t opaque,
                                                          const void *key,
                                                          uint16_t nkey,
                                                          uint64_t cas,
                                                          uint16_t vbucket,
                                                          uint64_t bySeqno,
                                                          uint64_t revSeqno)
{
    void *specific = getEngineSpecific(cookie);
    UprConsumer *consumer = NULL;
    if (specific == NULL) {
        // Create a new tap consumer...
        consumer = uprConnMap_->newConsumer(cookie);
        if (consumer == NULL) {
            LOG(EXTENSION_LOG_WARNING, "Failed to create new upr consumer. "
                "Force disconnect\n");
            return ENGINE_DISCONNECT;
        }
        storeEngineSpecific(cookie, consumer);
    }
    else {
        consumer = reinterpret_cast<UprConsumer *>(specific);
    }

    storeEngineSpecific(cookie, consumer);

    std::string k(static_cast<const char*>(key), nkey);
    ItemMetaData itemMeta(cas, DEFAULT_REV_SEQ_NUM, 0, 0);

    if (itemMeta.cas == 0) {
        itemMeta.cas = Item::nextCas();
    }
    itemMeta.revSeqno = (revSeqno != 0) ? revSeqno : DEFAULT_REV_SEQ_NUM;

    return ConnHandlerDelete(consumer, k, cookie, vbucket, true, itemMeta);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprExpiration(const void* cookie,
                                                            uint32_t opaque,
                                                            const void *key,
                                                            uint16_t nkey,
                                                            uint64_t cas,
                                                            uint16_t vbucket,
                                                            uint64_t bySeqno,
                                                            uint64_t revSeqno)
{
    return uprDeletion(cookie, opaque, key, nkey, cas,
                       vbucket, bySeqno, revSeqno);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprFlush(const void* cookie,
                                                       uint32_t opaque,
                                                       uint16_t vbucket)
{
    //dliao: flush per vbucket TODO
    LOG(EXTENSION_LOG_WARNING, "%s Received flush.\n");

    return flush(cookie, 0);
}

ENGINE_ERROR_CODE EventuallyPersistentEngine::uprSetVbucketState(const void* cookie,
                                                                 uint32_t opaque,
                                                                 uint16_t vbucket,
                                                                 vbucket_state_t state)
{
    return ENGINE_ENOTSUP;
}
