#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H
#define TABLE_SIZE 26

#include <pthread.h>
#include <stddef.h>

typedef struct KeyNode {
  char *key;
  char *value;
  int *notif_fds;
  size_t amount_of_subscriptions;
  struct KeyNode *next;
} KeyNode;

typedef struct HashTable {
  KeyNode *table[TABLE_SIZE];
  pthread_rwlock_t tablelock;
} HashTable;

/// Creates a new KVS hash table.
/// @return Newly created hash table, NULL on failure
struct HashTable *create_hash_table();

int hash(const char *key);

// Writes a key value pair in the hash table.
// @param ht The hash table.
// @param key The key.
// @param value The value.
// @return 0 if successful.
int write_pair(HashTable *ht, const char *key, const char *value);

// Reads the value of a given key.
// @param ht The hash table.
// @param key The key.
// return the value if found, NULL otherwise.
char *read_pair(HashTable *ht, const char *key);

/// Deletes a pair from the table.
/// @param ht Hash table to read from.
/// @param key Key of the pair to be deleted.
/// @return 0 if the node was deleted successfully, 1 otherwise.
int delete_pair(HashTable *ht, const char *key);

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);

/// Subscribes a key from the table.
/// @param ht Hash table to read from.
/// @param key Key of the pair to be subscribed.
/// @param notif_fd Notifications fd of the client that wants to subscribe the
/// key.
/// @return 0 if the node of that key does not exist, 1 if it exists.
char subscribe_table_key(HashTable *ht, const char *key, int notif_fd);

/// Subscribes a key from the table.
/// @param ht Hash table to read from.
/// @param key Key of the pair to be subscribed.
/// @param notif_fd Notifications fd of the client that wants to unsubscribe the
/// key.
/// @return 0 if the subscription existed and got removed, 1 if it didn't exist.
char unsubscribe_table_key(HashTable *ht, const char *key, int notif_fd);

/// Unsubscribes clients from all key.
/// @param ht Hash table to read from.
/// @param notif_fd Notifications fd of the client that wants to unsubscribe the
/// keys.
/// @return 0 if the unsubscription is successful, 1 if it isn't.
char global_unsubscribe(HashTable *ht, int notif_fd);

/// Unsubscribes every client from all keys.
/// @param ht Hash table to read from.
void unsubscribe_everyone(HashTable *ht);

#endif // KVS_H
