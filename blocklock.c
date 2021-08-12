
#define REDISMODULE_EXPERIMENTAL_API
#include "../redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static RedisModuleType *LockType;

typedef struct _BlockLockNode {
    unsigned long long clientid;
    long long timeout;
    RedisModuleTimerID tid;
    struct _BlockLockNode *next;
} BlockLockNode;

typedef struct _BlockLock {
    BlockLockNode *head;
    BlockLockNode *tail;
    size_t len;
    RedisModuleKey *key;
} BlockLock;

BlockLock* createBlockLock() {
    BlockLock *o;
    o = RedisModule_Alloc(sizeof(*o));
    o->head = o->tail = NULL;
    o->len = 0;
    return o;
}

BlockLockNode* pushBlockLock(BlockLock* lb, unsigned long long clientid, long long timeout) {
    BlockLockNode* node = RedisModule_Alloc(sizeof(*node));
    node->clientid = clientid;
    node->timeout = timeout;
    node->tid = 0;
    node->next = NULL;
    if (lb->tail) {
        lb->tail->next = node;
        lb->tail = node;
    } else {
        lb->head = lb->tail = node;
    }
    lb->len++;
    return node;
}

void popBlockLock(BlockLock* lb) {
    BlockLockNode* node = lb->head;
    if (node) {
        lb->head = node->next;
        if (lb->head == NULL) {
            lb->tail = NULL;
        }
        node->next = NULL;
        lb->len--;
        RedisModule_Free(node);
    }
}

int delBlockLock(BlockLock* lb, unsigned long long clientid) {
    BlockLockNode* node = lb->head;
    if (node && node->clientid == clientid) {
        popBlockLock(lb);
        return 1;
    }
    while (node) {
        BlockLockNode* temp = node->next;
        if (temp && temp->clientid == clientid) {
            if (temp == lb->tail) {
                lb->tail = node;
            }
            node->next = temp->next;
            lb->len--;
            RedisModule_Free(temp);
            return 1;
        }
        node = node->next;
    }
    return 0;
}

void unlockTimeout(RedisModuleCtx *ctx, void *data) {
    RedisModuleKey *key = (RedisModuleKey *)data;
    if (RedisModule_ModuleTypeGetType(key) != LockType) {
        return;
    }
    BlockLock *lb = RedisModule_ModuleTypeGetValue(key);
    popBlockLock(lb);
    if (lb->len == 0) {
        RedisModule_DeleteKey(key);
        return;
    }
    RedisModule_SignalKeyAsReady(ctx, (RedisModuleString*)RedisModule_GetKeyNameFromModuleKey(key));
}

void BlockLock_Disconnected(RedisModuleCtx *ctx, RedisModuleBlockedClient *bc) {
    REDISMODULE_NOT_USED(bc);
    
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
    RedisModuleString* keyname = (RedisModuleString*)RedisModule_GetBlockedClientPrivateData(ctx);
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyname, REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_ModuleTypeGetType(key) != LockType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return;
    }
    BlockLock *lb = RedisModule_ModuleTypeGetValue(key);
    if (lb->len == 0) {
        RedisModule_DeleteKey(key);
        return;
    }
    delBlockLock(lb, RedisModule_GetClientId(ctx));
}

int blockReplayCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argc);

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_ModuleTypeGetType(key) != LockType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }
    BlockLock *lb = RedisModule_ModuleTypeGetValue(key);
    if (lb->len == 0) {
        RedisModule_DeleteKey(key);
        return REDISMODULE_OK;
    }
    
    unsigned long long clientid = RedisModule_GetClientId(ctx);
        
    BlockLockNode *node = lb->head;
    // RedisModule_Log(ctx, "warning", "blockReplayCallback head: %lld, now: %lld",
    //     node->clientid, RedisModule_GetClientId(ctx));
    if (node->clientid != clientid) {
        RedisModule_CloseKey(key);
        return REDISMODULE_ERR;
    }
    node->tid = RedisModule_CreateTimer(ctx, node->timeout, unlockTimeout, key);
        RedisModule_CloseKey(key);
    RedisModule_ReplyWithLongLong(ctx, 1);
    return REDISMODULE_OK;
}

int Lock_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
    if (argc != 3) return RedisModule_WrongArity(ctx);
    long long timeout;

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);

    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(key) != LockType) {
        RedisModule_DeleteKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    if (RedisModule_StringToLongLong(argv[2], &timeout) != REDISMODULE_OK) {
        RedisModule_DeleteKey(key);
        return RedisModule_ReplyWithError(ctx,"ERR invalid timeout");
    }
    BlockLock *lb;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        lb = createBlockLock(key);
        RedisModule_ModuleTypeSetValue(key, LockType, lb);
    } else {
        lb = RedisModule_ModuleTypeGetValue(key);
    }

    if (lb->len == 0) {
        BlockLockNode* node = pushBlockLock(lb, RedisModule_GetClientId(ctx), timeout);
        node->tid = RedisModule_CreateTimer(ctx, timeout, unlockTimeout, key);
        RedisModule_ReplyWithLongLong(ctx, 1);
    } else {
        if (RedisModule_GetClientId(ctx) == lb->head->clientid) {
            RedisModule_ReplyWithLongLong(ctx, 0);
            return REDISMODULE_OK;
        }
        RedisModuleBlockedClient *bc = RedisModule_BlockClientOnKeys(ctx, blockReplayCallback, 
            NULL, NULL, 0, &argv[1], 1, argv[1]);
        RedisModule_SetDisconnectCallback(bc, BlockLock_Disconnected);
        pushBlockLock(lb, RedisModule_GetClientId(ctx), timeout);
    }
    return REDISMODULE_OK;
}

int Unlock_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */
    if (argc != 2) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1], REDISMODULE_READ|REDISMODULE_WRITE);

    if (RedisModule_ModuleTypeGetType(key) != LockType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    BlockLock *lb = RedisModule_ModuleTypeGetValue(key);
    if (lb->len == 0) {
        RedisModule_ReplyWithLongLong(ctx, 0);
    } else {
        BlockLockNode *node = lb->head;
        if (node->clientid != RedisModule_GetClientId(ctx)) {
            RedisModule_ReplyWithLongLong(ctx, 0);
        } else {
            if (node->tid != 0) {
                RedisModule_StopTimer(ctx, node->tid, NULL);
                node->tid = 0;
            }
            popBlockLock(lb);
            RedisModule_ReplyWithLongLong(ctx, 1);

            if (lb->len == 0) {
                RedisModule_DeleteKey(key);
                return REDISMODULE_OK;
            }
            node = lb->head;
            node->tid = RedisModule_CreateTimer(ctx, node->timeout, unlockTimeout, key);
            // RedisModule_SubscribeToServerEvent(node->ctx, RedisModuleEvent_ClientChange, clientChangeCallback);
            RedisModule_SignalKeyAsReady(ctx, argv[1]);
        }
    }
    return REDISMODULE_OK;
}

size_t BlockLockMemUsage(const void *value) {
    REDISMODULE_NOT_USED(value);
    // TODO:
    return 0;
}
void BlockLockFree(void *value) {
    BlockLock* o = (BlockLock*) value;
    BlockLockNode *cur, *next;
    cur = o->head;
    while(cur) {
        next = cur->next;
        RedisModule_Free(cur);
        cur = next;
    }
    RedisModule_Free(o);
}

/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    REDISMODULE_NOT_USED(argc);

    if (RedisModule_Init(ctx, "BlockLock", 1, REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;
        
    RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = NULL,
        .rdb_save = NULL,
        .aof_rewrite = NULL,
        .mem_usage = BlockLockMemUsage,
        .free = BlockLockFree,
        .digest = NULL
    };
    LockType = RedisModule_CreateDataType(ctx, "BlockLock", 0, &tm);
    if (LockType == NULL) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "lock",
        Lock_RedisCommand, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "unlock",
        Unlock_RedisCommand, "", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
