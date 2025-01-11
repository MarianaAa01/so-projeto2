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
#include <semaphore.h>

#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"
//#include "subscribed_keys_list.h"

pthread_cond_t buffer_full; // condicao para avisar as threads de que o buffer esta cheio
char boss_thread_buffer[121]; // buffer produtor-consumidor onde se guardam as mensagens de request
pthread_mutex_t buffer_mutex; // mutex para controlar as escritas no buffer produtor-consumidor

// to store whate we need in the client thread function
typedef struct clientInfo {
  int server_fd;
  char req_pipe_path[40];
  char resp_pipe_path[40];
  char notif_pipe_path[40];
  //subscribed_key **subscribed_keys_table;
} c_info;

// info partilhada entre threads
struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t session_mutex = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;

int filter_job_files(
    const struct dirent *entry) { // vê que files é que são do tipo ".job"
  const char *dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0) {
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
                       char *out_path) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
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
static int run_job(int in_fd, int out_fd, char *filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
    case CMD_WRITE:
      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values)) {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }
      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0) {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups) {
        wait(NULL);
      } else {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0) {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      } else if (aux == 1) {
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
static void *get_file(void *arguments) {
  // lê os argumentos
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  // lock para apenas uma thread ler de cada vez
  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  // lê a diretoria
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    // abre cada file para readOnly (in_fd)
    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }
    // cria o file output com writeOnly (out_fd)
    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    // processar os comandos do file
    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

static void *boss_thread(void *fd){
  // fd do servidor geral, utilizado para os requests
  int server_fd;
  server_fd = *(int *)fd;

  // criamos o semaforo que gere a quantidade de sessoes
  sem_open("sessions", 0, NULL, S);

  while(1){
    // vai lendo do server_fd
    if (read(server_fd, boss_thread_buffer, sizeof(boss_thread_buffer)) == -1) {
      write_str(STDERR_FILENO, "Read failed.\n");
      return NULL;
	} else {
		sem_wait("sessions");
		pthread_mutex_lock(&buffer_mutex);
		// Avisamos uma das threads de que o buffer esta cheio.
		// Essa thread desbloqueia o lock depois de utilizar o info no buffer.
		pthread_cond_signal(&buffer_full);
	}
  }
  sem_close("sessions");
}

static void *client_thread(void *fd) {
  int server_fd;
  server_fd = *(int *)fd;
  char succeeded[2];
  succeeded[OPCODE] = 1;
  succeeded[RESULT] = 0;

while (1){
 // lock para termos uma sessão de cada vez
  pthread_mutex_lock(&session_mutex);

  //ler mensagem
  while(boss_thread_buffer[0] == '\0')
	pthread_cond_wait(&buffer_full, &buffer_mutex);

  // verificar  OP_CODE é 1 (connection request)
  if (boss_thread_buffer[0] != 1) {
    write_str(STDERR_FILENO, "Invalid operation code in client request.\n");
    return NULL;
  }
  memset(boss_thread_buffer, '\0', sizeof(boss_thread_buffer));
  pthread_mutex_unlock(&buffer_mutex);
  char req_pipe_path[40];
  char resp_pipe_path[40];
  char notif_pipe_path[40];
  memcpy(req_pipe_path, boss_thread_buffer + 1, 40);
  memcpy(resp_pipe_path, boss_thread_buffer + 41, 40);
  memcpy(notif_pipe_path, boss_thread_buffer + 81, 40);
  // primeiro lemos o request
  int req_fd = open(req_pipe_path, O_RDONLY);
  if (req_fd == -1) {
    write_str(STDERR_FILENO, "Open failed\n");
    return NULL;
  }
  // escrever a resposta para o cliente
  int resp_fd = open(resp_pipe_path, O_WRONLY);
  if (resp_fd == -1) {
    close(req_fd);
    write_str(STDERR_FILENO, "Open failed\n");
    // Escreve para o FIFO de respostas que a conexão não foi feita com sucesso
    return NULL;
  }
  // escrever as notificações para o cliente
  int notif_fd = open(notif_pipe_path, O_WRONLY);
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
  char buffer[1 + MAX_STRING_SIZE];
  ssize_t bytes_read = read(req_fd, buffer, sizeof(buffer));
  while (buffer[0] != 2) { // sai daqui quando o cliente quer dar disconnect
    ssize_t bytes_read = read(req_fd, buffer, sizeof(buffer));
    if (bytes_read <= 0) {
      // se lermos um 0, breaks the loop
      break;
    }
    succeeded[OPCODE] = buffer[0];
    // faz algo consoante o OP_CODE recebido na request_message (buffer[0])
    switch (buffer[0]) {
    case 3: { // OP_CODE do subscribe
      // Obter a key que o cliente quer subscrever
      char s_key[MAX_STRING_SIZE];
      memcpy(s_key, buffer + 1, (size_t)(bytes_read - 1));
      s_key[bytes_read - 1] = '\0';
      //Subscrevemos a key pretendida
      succeeded[RESULT] = subscribe_key(s_key, notif_fd);
      write(resp_fd, &succeeded, sizeof(succeeded)); // responder ao cliente
      break;
    }
    case 4: { // OP_CODE do unsubscribe
      // Obter a key que o cliente quer dessubscrever
      char u_key[MAX_STRING_SIZE];
      memcpy(u_key, buffer + 1, (size_t)bytes_read - 1);
      u_key[bytes_read - 1] = '\0';
      //Subscrevemos a key pretendida
      succeeded[RESULT] = unsubscribe_key(u_key, notif_fd);
      write(resp_fd, &succeeded, sizeof(succeeded)); // Respond to the client
      break;
    }
    }
  }

  // fechar os file descriptors
  close(req_fd);
  close(resp_fd);
  close(notif_fd);
  // unlock
  sem_post("sessions");
  pthread_mutex_unlock(&session_mutex);
  return NULL;
}

}

static void dispatch_threads(DIR *dir) {
  // array de threads com o maximo de threads dada no input (max_threads)
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  // inicializar a SharedData entre essas threads
  struct SharedData thread_data = {dir, jobs_directory,
                                   PTHREAD_MUTEX_INITIALIZER};

  // ciclo for para criar threads, sendo que cada thread executa a get_file
  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) !=
        0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  // sincronizar as threads com o pthread_join
  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}

int main(int argc, char **argv) {
  // se os argumentos todos n forem apresentados o programa termina
  if (argc < 5) {
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
  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  // maximo de threads em simultaneo
  max_threads = strtoul(argv[2], &endptr, 10);
  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  const char *nome_do_FIFO_de_registo = argv[4];
  // criar o FIFO (named pipe) de registo, por onde os clientes iniciam sessão
  // no servidor
  if (mkfifo(nome_do_FIFO_de_registo, 0666) == -1) {
    write_str(STDERR_FILENO, "Failed creating FIFO.\n");
    printf("FIFO: %s | Errno: %d\n", nome_do_FIFO_de_registo, errno);
    return 1;
  } else {
    printf("FIFO '%s' was created!\n",
           nome_do_FIFO_de_registo); // printf só para debug
  }

  // temos de abrir o FIFO para leitura porque é assim que o servidor "ouve" o
  // que os clientes "querem"
  printf("Cliente nome_do_FIFO_de_registo: %s\n",
         nome_do_FIFO_de_registo); // DEBUG
  int server_fd = open(nome_do_FIFO_de_registo, O_RDWR);
  if (server_fd == -1) {
    write_str(STDERR_FILENO, "Failed opening FIFO.\n");
    unlink(nome_do_FIFO_de_registo); // tirar o FIFO em caso de erro
    return 1;
  }

  // criar a hash
  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    unlink(nome_do_FIFO_de_registo); // tirar o FIFO em caso de erro
    return 1;
  }

  // abrir a diretoria
  DIR *dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    unlink(nome_do_FIFO_de_registo); // tirar o FIFO em caso de erro
    return 0;
  }

  memset(boss_thread_buffer, '\0', sizeof(boss_thread_buffer));
  // init da condicao das threads
  pthread_cond_init(&buffer_full, NULL);

  // init da mutex do buffer produtor consumidor
  pthread_mutex_init(&buffer_mutex, NULL);

  // Criamos uma thread anfitria para organizar os pedidos de connect ao server
  pthread_t thread_boss;
  pthread_create(&thread_boss, NULL, boss_thread,
                 (void *)&server_fd);

  // Criamos uma thread para controlar a comunicaçao cliente-servidor (parte 2 -
  // exercicio 1.1, so 1 cliente)
  pthread_t thread_client[S];
  for (int i = 0; i < S; i++){
    pthread_create(&thread_client[i], NULL, client_thread,
                 (void *)&server_fd);
  }

  // cria e organiza/gerencia as threads como já vimos
  dispatch_threads(dir);
  // fechar a diretoria
  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    unlink(nome_do_FIFO_de_registo); // tirar o FIFO em caso de erro
    return 0;
  }

  for (int i = 0; i < S; i++){
    pthread_join(thread_client[i], NULL); // Da join da thread cliente
  }

  // sincronizar backups ativos
  /*cada vez que o backup é concluído, o active_backups é decrementado
  quando o active_backups chega a zero o kvs termina (com o kvs_terminate())
  */
  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  // fechar o FIFO após terminar de processar os pedidos dos clientes
  if (close(server_fd) == -1) {
    write_str(STDERR_FILENO, "Failed to close FIFO.\n");
    unlink(nome_do_FIFO_de_registo); // tirar o FIFO em caso de erro
    return 1;
  } else {
    printf("FIFO '%s' closed successfully!\n",
           nome_do_FIFO_de_registo); // printf para debug
  }

  kvs_terminate();
  unlink(nome_do_FIFO_de_registo); // tirar o FIFO em caso de erro
  pthread_cond_destroy(&buffer_full);
  pthread_mutex_destroy(&buffer_mutex);
  return 0;
}
