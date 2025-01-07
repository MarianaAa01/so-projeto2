#include "api.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/errno.h>

#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "../server/io.h"

int req_fd;
int notif_fd;
int resp_fd;

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path,
                int *notif_pipe) {

  // Criamos os fifos para comunicaçao entre processos
  if (mkfifo(req_pipe_path, 0666) == -1) {
    write_str(STDERR_FILENO, "Failed creating fifo_pedido.\n");
    //printf("Errno: %d\n", errno);
    return 1;
  }
  if (mkfifo(resp_pipe_path, 0666) == -1) {
    write_str(STDERR_FILENO, "Failed creating fifo_resposta.\n");
    // Em caso de erro, unlink dos fifos ja criados
    unlink(req_pipe_path);
    return 1;
  }
  if (mkfifo(notif_pipe_path, 0666) == -1) {
    write_str(STDERR_FILENO, "Failed creating fifo_notificacoes.\n");
    // Em caso de erro, unlink dos fifos ja criados
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    return 1;
  }
  
  // o client vai fazer um pedido então o fifo_pedido esta em modo de escrita
  printf("Cliente server_path: %s\n", server_pipe_path); //DEBUG
  int server_fd = open(server_pipe_path, O_WRONLY);
  if (server_fd == -1) {
    write_str(STDERR_FILENO, "Failed to open fifo_pedido for writing\n");
    // Em caso de erro, unlink dos fifos ja criados
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    unlink(notif_pipe_path);
    return 1;
  }


  // Podemos enviar a mensagem antes de fazer open, os fifos ja existem
  // Se enviarmos a mensagem no fim, os fifos ficam bloqueados nos opens por causa da ordem
  
  // mensagem de request (com o formato pedido) 
  char request_message[1 + 40 + 40 + 40];
  request_message[0] = 1; // OP_CODE=1 para connect
  snprintf(request_message + 1, 40, "%s", req_pipe_path);
  snprintf(request_message + 41, 40, "%s", resp_pipe_path);
  snprintf(request_message + 81, 40, "%s", notif_pipe_path);

  // mandar a mensagem construída
  if (write(server_fd, request_message, sizeof(request_message)) == -1) {
    write_str(STDERR_FILENO, "Failed to write connection request to server pipe\n");
    close(server_fd);
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    unlink(notif_pipe_path);
    return 1;
  }

  // temos de abrir o FIFO para leitura porque é assim que o servidor "ouve" o
  // que os clientes "querem"
  req_fd = open(req_pipe_path, O_WRONLY);
  if (req_fd == -1) {
    // Em caso de erro, unlink dos fifos já criados e close dos já abertos
    close(server_fd);
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    unlink(notif_pipe_path);
    write_str(STDERR_FILENO, "Failed opening FIFO.");
    return 1;
  }

  // o cliente vai obter uma resposta por isso isto está em modo de leitura
  resp_fd = open(resp_pipe_path, O_RDONLY);
  if (resp_fd == -1) {
    // Em caso de erro, unlink dos fifos já criados e close dos já abertos
    close(server_fd);
    close(req_fd);
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    unlink(notif_pipe_path);
    write_str(STDERR_FILENO, "Failed to open fifo_resposta for reading\n");
    return 1;
  }

  // o cliente vai obter uma notificação por isso isto está em modo de leitura
  notif_fd = open(notif_pipe_path, O_RDONLY);
  if (notif_fd == -1) {
    // Em caso de erro, unlink dos fifos já criados e close dos já abertos
    close(server_fd);
    close(req_fd);
    close(resp_fd);
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    unlink(notif_pipe_path);
    write_str(STDERR_FILENO, "Failed to open fifo_notificacoes for reading\n");
    return 1;
  }

  // TODOS OS FIFOS FORAM CRIADOS E ABERTOS COM SUCESSO, MOVING ON PARA AS MENSAGENS

  // ler a resposta do servidor
  char response[2]; // OP_CODE + result
  if (read(resp_fd, response, sizeof(response)) != sizeof(response)) {
    write_str(STDERR_FILENO, "Failed to read response from server\n");
    close(req_fd);
    close(resp_fd);
    close(notif_fd);
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    unlink(notif_pipe_path);
    return 1;
  }

  // validar a resposta
  if (response[0] != 1 || response[1] != 0) {
    write_str(STDERR_FILENO, "Connection failed with server\n");
    close(req_fd);
    close(resp_fd);
    close(notif_fd);
    unlink(req_pipe_path);
    unlink(resp_pipe_path);
    unlink(notif_pipe_path);
    return 1;
  } else {
    char message[41] = "Server returned ";
    message[16] = '0' + response[1];
    strncat(message, " for operation: connect", sizeof(message) - strlen(message) - 1);
    write_str(STDOUT_FILENO, message);
  }

  // Finalização após processar a resposta
  close(server_fd);
  close(req_fd);
  close(resp_fd);
  close(notif_fd);
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);

  close(server_fd);

  return 0;
}

int kvs_disconnect (char const *req_pipe_path, char const *resp_pipe_path,
                char const *notif_pipe_path){
  // close pipes and unlink pipe files
  // fechar os pipes
  close(req_fd);
  close(resp_fd);
  close(notif_fd);

  // unlink the FIFOs after successful connection
  unlink(req_pipe_path);
  unlink(resp_pipe_path);
  unlink(notif_pipe_path);
  return 0;
}

int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  // pipe

  //mensagem de request
  char subscribe_request[1+MAX_STRING_SIZE];
  subscribe_request[0]=2;//OP_CODE=2 para subscribe
  snprintf(subscribe_request + 1, MAX_STRING_SIZE, "%s", key);
  //mandar a mensagem 
  if(write(req_fd,subscribe_request,sizeof(subscribe_request)==-1)){
    write_str(STDERR_FILENO,"Failed to write subscription request to server pipe\n");
    return 1;
  }
  // ler a resposta do server
  char response[2];// OP_CODE + result
  if (read(resp_fd, response, sizeof(response)) != sizeof(response)) {
    write_str(STDERR_FILENO, "Failed to read response from server\n");
    return 1;
  }
  // validar a resposta recebida
  if (response[0] != 2) {
    write_str(STDERR_FILENO, "Invalid response code from server\n");
    return 1;
  }
  // result da subscription
  char message[50];
  snprintf(message, sizeof(message), "Server returned %d for operation: subscribe\n", response[1]);
  write_str(STDOUT_FILENO, message);
  return 0;
}

int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  // pipe

  // mensagem de unsubscribe
  char unsubscribe_request[1+MAX_STRING_SIZE];
  unsubscribe_request[0]=3;//OP_CODE=3 para unsubscribe
  snprintf(unsubscribe_request + 1, MAX_STRING_SIZE, "%s", key);
  return 0;
  // mandar o request
  if (write(req_fd, unsubscribe_request, sizeof(unsubscribe_request)) == -1) {
    write_str(STDERR_FILENO, "Failed to write unsubscribe request to request pipe\n");
    return 1;
  }
  // ler a resposta do server
  char response[2]; // OP_CODE + result
  if (read(resp_fd, response, sizeof(response)) != sizeof(response)) {
    write_str(STDERR_FILENO, "Failed to read response from server\n");
    return 1;
  }
  // validar a resposta recebida
  if (response[0] != 3) {
    write_str(STDERR_FILENO, "Invalid response code from server\n");
    return 1;
  }
  // resultado da unsubscribe
  char message[50];
  snprintf(message, sizeof(message), "Server returned %d for operation: unsubscribe\n", response[1]);
  write_str(STDOUT_FILENO, message);

  return 0;
}
