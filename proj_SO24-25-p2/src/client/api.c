#include "api.h"
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>

#include "src/common/constants.h"
#include "src/common/protocol.h"

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path, int* notif_pipe) {

  //criação dos pipes de pedido, resposta e notificações
  if (mkfifo(req_pipe_path, 0666) == -1) {
    write_str(STDERR_FILENO, "Failed creating fifo_pedido.\n");
    return 1;
  }
  if (mkfifo(resp_pipe_path, 0666) == -1) {
    write_str(STDERR_FILENO, "Failed creating fifo_resposta.\n");
    return 1;
  }
  if (mkfifo(notif_pipe_path, 0666) == -1) {
    write_str(STDERR_FILENO, "Failed creating fifo_notificacoes.\n");
    return 1;
  }

  //o client vai fazer um pedido então o fifo_pedido ta em modo de escrita
  int req_fd = open(req_pipe_path, O_WRONLY);
  if (req_fd == -1) {
    write_str(STDERR_FILENO, "Failed to open fifo_pedido for writing\n");
    return 1;
  }
  //o cliente vai obter uma resposta por isso isto está em modo de leitura
  int resp_fd = open(resp_pipe_path, O_RDONLY);
  if (resp_fd == -1) {
    write_str(STDERR_FILENO, "Failed to open fifo_resposta for reading\n");
    return 1;
  }
  //o cliente vai obter uma notificação por isso isto está em modo de leitura
  int notif_fd = open(notif_pipe_path, O_RDONLY);
  if (notif_fd == -1) {
    write_str(STDERR_FILENO, "Failed to open fifo_notificacoes for reading\n");
    return 1;
  }
  
  //temos de abrir o FIFO para leitura porque é assim que o servidor "ouve" o que os clientes "querem"
  int server_fd = open(server_pipe_path, O_WRONLY);
  if (server_fd == -1) {
    write_str(STDERR_FILENO, "Failed opening FIFO.\n");
    unlink(server_pipe_path); //tirar o FIFO em caso de erro
    return 1;
  }
    
  //mensagem de request (com o formato pedido)
  char request_message[1 + 40 + 40 + 40];
  request_message[0] = 1; // OP_CODE=1 para connect
  snprintf(request_message + 1, 40, "%s", req_pipe_path);
  snprintf(request_message + 41, 40, "%s", resp_pipe_path);
  snprintf(request_message + 81, 40, "%s", notif_pipe_path);

  //mandar a mensagem construída
  if (write(server_fd, request_message, sizeof(request_message)) == -1) {
      write_str(STDERR_FILENO, "Failed to write connection request to server pipe\n");
      close(req_fd);
      close(resp_fd);
      close(notif_fd);
      close(server_fd);
      return 1;
  }

  //ler a resposta do servidor
  char response[2]; // OP_CODE + result
  if (read(resp_fd, response, sizeof(response)) != sizeof(response)) {
      write_str(STDERR_FILENO, "Failed to read response from server\n");
      close(req_fd);
      close(resp_fd);
      close(notif_fd);
      return 1;
  }

  //validar a resposta
  if (response[0] != 1 || response[1] != 0) {
      write_str(STDERR_FILENO, "Connection failed with server\n");
      close(req_fd);
      close(resp_fd);
      close(notif_fd);
      return 1;
  }
  
  close(server_fd);

  return 0;
}
 
int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  //fechar os pipes
  close(req_fd);
  close(resp_fd);
  close(notif_fd);

  //unlink the FIFOs after successful connection
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);
  return 0;
}

int kvs_subscribe(const char* key) {
  // send subscribe message to request pipe and wait for response in response pipe
  return 0;
}

int kvs_unsubscribe(const char* key) {
    // send unsubscribe message to request pipe and wait for response in response pipe
  return 0;
}


