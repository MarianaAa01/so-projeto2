#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <signal.h>

#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"
// #include "subscribed_keys_list.h"

// to store whate we need in the client thread function
typedef struct clientInfo
{
  int server_fd;
  char req_pipe_path[40];
  char resp_pipe_path[40];
  char notif_pipe_path[40];
  // subscribed_key **subscribed_keys_table;
} c_info;

// info partilhada entre threads
struct SharedData
{
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t buffer_mutex = PTHREAD_MUTEX_INITIALIZER; // protege o buffer produtor-consumidor
pthread_cond_t buffer_cond = PTHREAD_COND_INITIALIZER; // protege o buffer produtor-consumidor


size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;

char buffer[121]; // buffer produtor-consumidor

int filter_job_files(
    const struct dirent *entry)
{ // vê que files é que são do tipo ".job"
  const char *dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0)
  {
    return 1; // Keep this file (it has the .job extension)
  }
  return 0;
}

/*
entry_files: vê se o file é válido
             vê se o nome do file respeita o MAX_JOB_FILE_NAME_SIZE
             controi o output file
*/
static int entry_files(const char *dir, struct dirent *entry, char *in_path,
                       char *out_path)
{
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job"))
  {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE)
  {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

// era a nossa tableOperations (processa os comandos)
static int run_job(int in_fd, int out_fd, char *filename)
{
  size_t file_backups = 0;
  while (1)
  {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd))
    {
    case CMD_WRITE:
      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0)
      {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values))
      {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }
      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0)
      {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd))
      {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0)
      {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd))
      {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1)
      {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0)
      {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups)
      {
        wait(NULL);
      }
      else
      {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0)
      {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      }
      else if (aux == 1)
      {
        return 1;
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      write_str(STDOUT_FILENO,
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      printf("EOF\n");
      return 0;
    }
  }
}

// executada dentro de cada thread
// frees arguments
static void *get_file(void *arguments)
{
  // lê os argumentos
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0)
  {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  // lê a diretoria
  while ((entry = readdir(dir)) != NULL)
  {
    if (entry_files(dir_name, entry, in_path, out_path))
    {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0)
    {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    // abre cada file para readOnly (in_fd)
    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1)
    {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }
    // cria o file output com writeOnly (out_fd)
    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1)
    {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    // processar os comandos do file
    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out)
    {
      if (closedir(dir) == -1)
      {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0)
    {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0)
  {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

static void *host_thread(void *fd) {
    int server_fd;
    server_fd = *(int *)fd;
    while (1) {
        // Bloqueamos o mutex que protege o buffer produtor-consumidor
        pthread_mutex_lock(&buffer_mutex);
        while (buffer[0] != '\0') {
            // Enquanto o buffer não for esvaziado, a host_thread
            // espera que outra thread sinalize que já é seguro
            // reescrever no buffer
            pthread_cond_wait(&buffer_cond, &buffer_mutex);
        }
        if (read(server_fd, buffer, 121) == -1) {
            write_str(STDERR_FILENO, "Read failed.\n");
            pthread_mutex_unlock(&buffer_mutex);
            return NULL;
        }
        // Desbloqueamos o mutex e "acordamos" uma das client_threads
        // para processar o que está no buffer
        pthread_cond_signal(&buffer_cond);
        pthread_mutex_unlock(&buffer_mutex);
    }
    return NULL;
}

static void *client_thread(void *arg_struct) {
  //BLOQUEAR SIGUSR1 NAS THREADS DE CLIENTES
  sigset_t signal_mask;
  sigemptyset(&signal_mask); //inicialização da signal_mask vazia
  sigaddset(&signal_mask, SIGUSR1); //adiciona o sinal SIGUSR1 à signal_mask (signals que vão ser bloqueados)
  //usar pthread_sigmask() com a operação SIG_BLOCK
  if (pthread_sigmask(SIG_BLOCK, &signal_mask, NULL) != 0)
  {
    perror("Error setting signal mask");
    return NULL;
  }

    c_info client_information;
    client_information = *(c_info *)arg_struct;
    char succeeded[2];
    succeeded[OPCODE] = 1;
    succeeded[RESULT] = 0;
    char req_buffer[121];
    int buffer_escrito = 0;

    // ler request_message do cliente
    pthread_mutex_lock(&buffer_mutex);
    while (buffer[0] == '\0') {
        // Enquanto o buffer estiver vazio, a client_thread que a host_thread
        // sinalize que é seguro escrever no buffer
        pthread_cond_wait(&buffer_cond, &buffer_mutex);
    }

    // verificar  OP_CODE é 1 (connection request)
    if (buffer[0] != 1) {
        write_str(STDERR_FILENO, "Invalid operation code in client request.\n");
        pthread_mutex_unlock(&buffer_mutex);
        return NULL;
    }
    memcpy(client_information.req_pipe_path, buffer + 1, 40);
    memcpy(client_information.resp_pipe_path, buffer + 41, 40);
    memcpy(client_information.notif_pipe_path, buffer + 81, 40);
    // Esvaziamos o buffer produtor-consumidor, pois já foi processada
    // a informação que lá estava
    memset(buffer, '\0', sizeof(buffer));
    // Desbloqueamos o mutex e "acordamos" a host_thread para voltar
    // a receber requests de início de sessão
    pthread_cond_signal(&buffer_cond);
    pthread_mutex_unlock(&buffer_mutex);

    // primeiro lemos o request
    int req_fd = open(client_information.req_pipe_path, O_RDONLY);
    if (req_fd == -1) {
        write_str(STDERR_FILENO, "Open failed\n");
        return NULL;
    }
    // escrever a resposta para o cliente
    int resp_fd = open(client_information.resp_pipe_path, O_WRONLY);
    if (resp_fd == -1) {
        close(req_fd);
        write_str(STDERR_FILENO, "Open failed\n");
        // Escreve para o FIFO de respostas que a conexão não foi feita com sucesso
        return NULL;
    }
    // escrever as notificações para o cliente
    int notif_fd = open(client_information.notif_pipe_path, O_WRONLY);
    if (notif_fd == -1) {
        close(req_fd);
        close(resp_fd);
        write_str(STDERR_FILENO, "Open failed\n");
        // Escreve para o FIFO de respostas que a conexão não foi feita com sucesso
        succeeded[RESULT] = 1;
        write(resp_fd, &succeeded, sizeof(succeeded));
        return NULL;
    }
    // Escreve para o FIFO de respostas que a conexão foi feita com sucesso
    write(resp_fd, &succeeded, sizeof(succeeded));

    // loop para estar sempre a ler e a responder a requests de clients
    memset(req_buffer, '\0', sizeof(req_buffer));
    while (req_buffer[0] != 2) { // sai daqui quando o cliente quer dar disconnect
        ssize_t bytes_read = read(req_fd, req_buffer, sizeof(req_buffer));
        if (bytes_read <= 0) {
            // se lermos um 0, breaks the loop
            break;
        }
        succeeded[OPCODE] = req_buffer[0];
        // faz algo consoante o OP_CODE recebido na request_message (req_buffer[0])
        switch (req_buffer[0]) {
        case 3: { // OP_CODE do subscribe
            // Obter a key que o cliente quer subscrever
            char s_key[MAX_STRING_SIZE];
            memcpy(s_key, req_buffer + 1, (size_t)(bytes_read - 1));
            s_key[bytes_read - 1] = '\0';
            // Subscrevemos a key pretendida
            succeeded[RESULT] = subscribe_key(s_key, notif_fd);
            write(resp_fd, &succeeded, sizeof(succeeded)); // responder ao cliente
            break;
        }
        case 4: { // OP_CODE do unsubscribe
            // Obter a key que o cliente quer dessubscrever
            char u_key[MAX_STRING_SIZE];
            memcpy(u_key, req_buffer + 1, (size_t)bytes_read - 1);
            u_key[bytes_read - 1] = '\lo';
            // Subscrevemos a key pretendida
            succeeded[RESULT] = unsubscribe_key(u_key, notif_fd);
            write(resp_fd, &succeeded, sizeof(succeeded)); // Respond to the client
            break;
        }
        }
    }
    char message[2];
    message[OPCODE] = 2;
    message[RESULT] = 0;
    write(resp_fd, &message, sizeof(message)); // manda ao cliente o result
    unsubscribe_client(notif_fd);

    // fechar os file descriptors
    close(req_fd);
    close(resp_fd);
    close(notif_fd);
    return NULL;
}

static void dispatch_threads(DIR *dir)
{
  // array de threads com o maximo de threads dada no input (max_threads)
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL)
  {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  // inicializar a SharedData entre essas threads
  struct SharedData thread_data = {dir, jobs_directory,
                                   PTHREAD_MUTEX_INITIALIZER};

  // ciclo for para criar threads, sendo que cada thread executa a get_file
  for (size_t i = 0; i < max_threads; i++)
  {
    if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) !=
        0)
    {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  // sincronizar as threads com o pthread_join
  for (unsigned int i = 0; i < max_threads; i++)
  {
    if (pthread_join(threads[i], NULL) != 0)
    {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0)
  {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}


  //SIGNAL_HANDLER
  void sig_handler(int signal) {
    if (signal == SIGUSR1) {
      printf("Recebi o sinal SIGUSR1\n");
      //eliminar todas as subscrições de todos os clientes da hashtable (o array de notif_fds)
      //encerrar todos os FIFOs de notificações e de resposta dos clientes(para cada client faço close(notif_fd) e close(resp_fd))

      //O SERVER NÃO TERMINA
      //printf("Handled SIGUSR1: All subscriptions removed and FIFOs closed.\n"); //para debug
    } else {
      printf("Recebi um sinal inesperado: %d\n", signal); //debug
    }
  }


int main(int argc, char **argv)
{
  //receber aqui o signal e mandar pro sig_handler
  //(acho que usar o sigaction é melhor mas tava a dar bue erro e como só damos handle a um signal n faz tanta diferença)
  signal(SIGUSR1, sig_handler);

  // se os argumentos todos n forem apresentados o programa termina
  if (argc < 5)
  {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups>");
    write_str(STDERR_FILENO, " <nome_do_FIFO_de_registo> \n");
    return 1;
  }

  // inicializar as variaveis globais
  jobs_directory = argv[1];

  char *endptr;
  // maximo de backups simultaneos
  max_backups = strtoul(argv[3], &endptr, 10);
  if (*endptr != '\0')
  {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  // maximo de threads em simultaneo
  max_threads = strtoul(argv[2], &endptr, 10);
  if (*endptr != '\0')
  {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0)
  {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0)
  {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  const char *nome_do_FIFO_de_registo = argv[4];
  // criar o FIFO (named pipe) de registo, por onde os clientes iniciam sessão
  // no servidor
  if (mkfifo(nome_do_FIFO_de_registo, 0666) == -1)
  {
    write_str(STDERR_FILENO, "Failed creating FIFO.\n");
    printf("FIFO: %s | Errno: %d\n", nome_do_FIFO_de_registo, errno);
    return 1;
  }
  else
  {
    printf("FIFO '%s' was created!\n",
           nome_do_FIFO_de_registo); // printf só para debug
  }

  // temos de abrir o FIFO para leitura porque é assim que o servidor "ouve" o
  // que os clientes "querem"
  printf("Cliente nome_do_FIFO_de_registo: %s\n",
         nome_do_FIFO_de_registo); // DEBUG
  int server_fd = open(nome_do_FIFO_de_registo, O_RDWR);
  if (server_fd == -1)
  {
    write_str(STDERR_FILENO, "Failed opening FIFO.\n");
    unlink(nome_do_FIFO_de_registo); // tirar o FIFO em caso de erro
    return 1;
  }

  // criar a hash
  if (kvs_init())
  {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    unlink(nome_do_FIFO_de_registo); // tirar o FIFO em caso de erro
    return 1;
  }

  // abrir a diretoria
  DIR *dir = opendir(argv[1]);
  if (dir == NULL)
  {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    unlink(nome_do_FIFO_de_registo); // tirar o FIFO em caso de erro
    return 0;
  }

  // Criamos uma estrutura para conseguir enviar as informaçoes a thread cliente

  c_info single_client_info;

  single_client_info.server_fd = server_fd;

  //Colocamos o buffer a nulo
  memset(buffer, '\0', sizeof(buffer));

  //Lançamos a thread anfitria
  pthread_t thread_host;
  pthread_create(&thread_host, NULL, host_thread,
                 (void *)&server_fd);  

  // Criamos uma thread para controlar a comunicaçao cliente-servidor (parte 2 -
  // exercicio 1.1, so 1 cliente)
  pthread_t thread_client[S];
  for (int i = 0; i < S; i++){
    pthread_create(&(thread_client[i]), NULL, client_thread,
                 (void *)&single_client_info);
  }


  // cria e organiza/gerencia as threads como já vimos
  dispatch_threads(dir);
  // fechar a diretoria
  if (closedir(dir) == -1)
  {
    fprintf(stderr, "Failed to close directory\n");
    unlink(nome_do_FIFO_de_registo); // tirar o FIFO em caso de erro
    return 0;
  }

  for (int i = 0; i < S; i++){
    pthread_join(thread_client[i], NULL);
  }
  pthread_join(thread_host, NULL); // Da join da thread cliente

  // sincronizar backups ativos
  /*cada vez que o backup é concluído, o active_backups é decrementado
  quando o active_backups chega a zero o kvs termina (com o kvs_terminate())
  */
  while (active_backups > 0)
  {
    wait(NULL);
    active_backups--;
  }

  // fechar o FIFO após terminar de processar os pedidos dos clientes
  if (close(server_fd) == -1)
  {
    write_str(STDERR_FILENO, "Failed to close FIFO.\n");
    unlink(nome_do_FIFO_de_registo); // tirar o FIFO em caso de erro
    return 1;
  }
  else
  {
    printf("FIFO '%s' closed successfully!\n",
           nome_do_FIFO_de_registo); // printf para debug
  }

  kvs_terminate();
  unlink(nome_do_FIFO_de_registo); // tirar o FIFO em caso de erro
  return 0;
}