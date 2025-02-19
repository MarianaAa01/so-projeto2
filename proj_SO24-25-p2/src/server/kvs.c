#include "kvs.h"
#include "string.h"
#include <ctype.h>
#include <unistd.h>

#include <stdio.h>
#include <stdlib.h>

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of
// the project
int hash(const char *key) {

  int firstLetter = tolower(key[0]);
  if (firstLetter >= 'a' && firstLetter <= 'z') {
    return firstLetter - 'a';
  } else if (firstLetter >= '0' && firstLetter <= '9') {
    return firstLetter - '0';
  }
  return -1; // Invalid index for non-alphabetic or number strings
}

struct HashTable *create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht)
    return NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
    ht->table[i] = NULL;
  }
  pthread_rwlock_init(&ht->tablelock, NULL);
  return ht;
}

int write_pair(HashTable *ht, const char *key, const char *value) {
  int index = hash(key);

  // Search for the key node
  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      // overwrite value
      free(keyNode->value);
      keyNode->value = strdup(value);

      // Update clients
      for (int i = 0; i < keyNode->amount_of_subscriptions; i++) {
        char buffer[82];
        memset(buffer, '\0', sizeof(buffer));
        memcpy(buffer, keyNode->key, strlen(key));
        memcpy(buffer + 41, keyNode->value, strlen(value));
        size_t bytes_written = 0;
        while (bytes_written != 82) {
          bytes_written += write(keyNode->notif_fds[i], buffer, 82);
        }
      }

      return 0;
    }
    previousNode = keyNode;
    keyNode = previousNode->next; // Move to the next node
  }
  // Key not found, create a new key node
  keyNode = malloc(sizeof(KeyNode));
  keyNode->key = strdup(key);     // Allocate memory for the key
  keyNode->value = strdup(value); // Allocate memory for the value
  keyNode->amount_of_subscriptions = 0;
  keyNode->notif_fds = NULL;
  keyNode->next = ht->table[index]; // Link to existing nodes
  ht->table[index] = keyNode; // Place new key node at the start of the list
  return 0;
}

char *read_pair(HashTable *ht, const char *key) {
  int index = hash(key);

  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;
  char *value;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      value = strdup(keyNode->value);
      return value; // Return the value if found
    }
    previousNode = keyNode;
    keyNode = previousNode->next; // Move to the next node
  }

  return NULL; // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
  int index = hash(key);

  // Search for the key node
  KeyNode *keyNode = ht->table[index];
  KeyNode *prevNode = NULL;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      // Key found; delete this node
      if (prevNode == NULL) {
        // Node to delete is the first node in the list
        ht->table[index] =
            keyNode->next; // Update the table to point to the next node
      } else {
        // Node to delete is not the first; bypass it
        prevNode->next =
            keyNode->next; // Link the previous node to the next node
      }

      // Update clients
      for (int i = 0; i < keyNode->amount_of_subscriptions; i++) {
        char buffer[82];
        memset(buffer, '\0', sizeof(buffer));
        memcpy(buffer, keyNode->key, strlen(key));
        memcpy(buffer + 41, "DELETE", strlen("DELETE"));
        size_t bytes_written = 0;
        while (bytes_written != 82) {
          bytes_written += write(keyNode->notif_fds[i], buffer, 82);
        }
      }

      // Free the memory allocated for the key and value
      free(keyNode->key);
      free(keyNode->value);
      free(keyNode->notif_fds);
      free(keyNode); // Free the key node itself
      return 0;      // Exit the function
    }
    prevNode = keyNode;      // Move prevNode to current node
    keyNode = keyNode->next; // Move to the next node
  }

  return 1;
}

void free_table(HashTable *ht) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = ht->table[i];
    while (keyNode != NULL) {
      KeyNode *temp = keyNode;
      keyNode = keyNode->next;
      free(temp->key);
      free(temp->value);
      free(temp);
    }
  }
  pthread_rwlock_destroy(&ht->tablelock);
  free(ht);
}

char subscribe_table_key(HashTable *ht, const char *key, int notif_fd) {
  int index = hash(key);

  // Search for the key node
  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      // Encontrou a key na tabela. Vamos procurar se o fd já lá está
      for (int i = 0; i < keyNode->amount_of_subscriptions; i++) {
        if (notif_fd == keyNode->notif_fds[i]) {
          // Esta key já estava a ser subscrita por este cliente. Não fazemos
          // nada, mas operação teve sucesso.
          return 1;
        }
      }
      // A key existe mas não estava a ser seguida por este cliente.
      // Acrescentamos este notif_fd a esta key.
      int *new_fds =
          realloc(keyNode->notif_fds,
                  sizeof(int) * (keyNode->amount_of_subscriptions + 1));
      if (!new_fds) {
        perror("Realloc failed");
        return 0;
      }
      keyNode->notif_fds = new_fds;
      keyNode->notif_fds[keyNode->amount_of_subscriptions] = notif_fd;
      keyNode->amount_of_subscriptions++;
      return 1;
    }
    previousNode = keyNode;
    keyNode = previousNode->next; // Move to the next node
  }
  return 0;
}

int *remove_one_from_int_array(int *array, int size, int to_remove) {
  int *tmp_array = malloc(sizeof(int) * (size - 1));
  if (!tmp_array) {
    perror("Malloc failed");
    return array; // Retorna o array original em caso de falha
  }
  int j = 0;
  for (int i = 0; i < size; i++) {
    if (array[i] != to_remove) {
      tmp_array[j++] = array[i];
    }
  }
  free(array);
  return tmp_array;
}

char unsubscribe_table_key(HashTable *ht, const char *key, int notif_fd) {
  int index = hash(key);

  // Search for the key node
  KeyNode *keyNode = ht->table[index];
  KeyNode *previousNode;

  while (keyNode != NULL) {
    if (strcmp(keyNode->key, key) == 0) {
      // Encontrou a key na tabela. Vamos procurar se o fd já lá está
      for (int i = 0; i < keyNode->amount_of_subscriptions; i++) {
        if (notif_fd == keyNode->notif_fds[i]) {
          // Esta key estava a ser subscrita por este cliente. Removemos a
          // subscrição.
          keyNode->notif_fds = remove_one_from_int_array(
              keyNode->notif_fds, keyNode->amount_of_subscriptions, notif_fd);
          keyNode->amount_of_subscriptions--;
          return 0;
        }
      }
    }
    previousNode = keyNode;
    keyNode = previousNode->next; // Move to the next node
  }
  return 1;
}

char global_unsubscribe(HashTable *ht, int notif_fd) {
  pthread_rwlock_wrlock(&ht->tablelock); // lock the table for writing
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = ht->table[i];
    while (keyNode != NULL) {
      // verificar se o notif_fd está presente nos fds da key
      for (size_t j = 0; j < keyNode->amount_of_subscriptions; j++) {
        if (keyNode->notif_fds[j] == notif_fd) {
          // remover o notif_fd do array de fds da key
          for (size_t k = j; k < keyNode->amount_of_subscriptions - 1; k++) {
            keyNode->notif_fds[k] = keyNode->notif_fds[k + 1];
          }
          keyNode->amount_of_subscriptions--;
          keyNode->notif_fds =
              realloc(keyNode->notif_fds,
                      sizeof(int) * keyNode->amount_of_subscriptions);
          if (keyNode->notif_fds == NULL) {
            pthread_rwlock_unlock(&ht->tablelock);
            return 1; // único erro I think
          }
          break;
        }
      }
      keyNode = keyNode->next;
    }
  }

  pthread_rwlock_unlock(&ht->tablelock); // unlock the table
  return 0;
}

void unsubscribe_everyone(HashTable *ht) {
  pthread_rwlock_wrlock(&ht->tablelock); // lock the table for writing
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = ht->table[i];
    if (keyNode != NULL) {
      // esvazia o array (mantendo-o alocado) e colocar amount_of_subscriptions
      // a zero
      memset(keyNode->notif_fds, 0,
             sizeof(int) * keyNode->amount_of_subscriptions);
      keyNode->amount_of_subscriptions = 0;
    }
  }
  pthread_rwlock_unlock(&ht->tablelock); // unlock the table
}