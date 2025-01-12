#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

void write_str(int fd, const char *str) { write(fd, str, strlen(str)); }

void *get_notifications(void *fd)
{
  char buffer[82]; // buffer to hold message
  memset(buffer, '\0', sizeof(buffer));
  char key[41];   // buffer to hold key
  char value[41]; // buffer to hold value
  int *notif_fd;
  notif_fd = (int *)fd;
  while (read(*notif_fd, buffer, 82))
  {
    memcpy(key, buffer, 41);
    memcpy(value, buffer + 41, 41);
    size_t bytes_written = 0;
    write(STDOUT_FILENO, "(", 1);
    while (bytes_written != 41)
      bytes_written += write(STDOUT_FILENO, key, 41);
    bytes_written = 0;
    write(STDOUT_FILENO, ",", 1);
    while (bytes_written != 41)
      bytes_written += write(STDOUT_FILENO, value, 41);
    write(STDOUT_FILENO, ")", 1);
    memset(buffer, '\0', sizeof(buffer));
    //(<key>,<value>)
  }
  return NULL;
}

int main(int argc, char *argv[])
{
  if (argc < 3)
  {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n",
            argv[0]);
    return 1;
  }

  char req_pipe_path[256] = "/tmp/req";     // FIFO de pedido
  char resp_pipe_path[256] = "/tmp/resp";   // FIFO de resposta
  char notif_pipe_path[256] = "/tmp/notif"; // FIFO de notificaçao
  char *server_pipe_path = argv[2];         // FIFO do servidor (inicios de sessao)
  int notif_pipe;

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

  // conectar ao server
  if (kvs_connect(req_pipe_path, resp_pipe_path, server_pipe_path,
                  notif_pipe_path, &notif_pipe) != 0)
  {
    fprintf(stderr, "Failed to connect to the server.\n");
    return 1;
  }

  pthread_t notifications_receiver;
  pthread_create(&notifications_receiver, NULL, get_notifications, (void *)&notif_pipe);

  while (1)
  {
    switch (get_next(STDIN_FILENO))
    {
    case CMD_DISCONNECT:
      if (kvs_disconnect(req_pipe_path, resp_pipe_path, notif_pipe_path) !=
          0)
      {
        fprintf(stderr, "Failed to disconnect to the server\n");
        return 1;
      }
      return 0;

    case CMD_SUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_subscribe(keys[0]))
      {
        fprintf(stderr, "Command subscribe failed\n");
      }
      else
      {
        // TODO: start notifications thread
      }

      break;

    case CMD_UNSUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_unsubscribe(keys[0]))
      {
        fprintf(stderr, "Command subscribe failed\n");
      }

      break;

    case CMD_DELAY:
      if (parse_delay(STDIN_FILENO, &delay_ms) == -1)
      {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay_ms > 0)
      {
        printf("Waiting...\n");
        delay(delay_ms);
      }
      break;

    case CMD_INVALID:
      fprintf(stderr, "Invalid command. See HELP for usage\n");
      break;

    case CMD_EMPTY:
      break;

    case EOC:
      // input should end in a disconnect, or it will loop here forever
      // Finalização após processar a resposta
      if (kvs_disconnect(req_pipe_path, resp_pipe_path, notif_pipe_path) !=
          0)
      {
        fprintf(stderr, "Failed to disconnect to the server\n");
        return 1;
      }

      break;
    }
  }
}
