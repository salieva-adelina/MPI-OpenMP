#include "../../common/lib.h"
#include "mpi.h"

const uint64_t M = 8192; //N=xM ALWAYS!

struct in_addr lookup_host(const char *host)
{
    struct addrinfo hints, *res;
    int errcode;
    void *ptr;
    char buf[1024];
    struct in_addr retval;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = 0;

    CHECK(getaddrinfo(host, NULL, &hints, &res), -1, "Can't retrieve address info")

    printf("Host: %s\n", host);
    retval = ((struct sockaddr_in *)res->ai_addr)->sin_addr;
    //here it should be usable for sockets
    inet_ntop(res->ai_family, &retval, buf, BUFSIZE - 1);
    printf("IPv4 address: %s\n", buf);
    freeaddrinfo(res);
    return retval;
}

void AVG(const double *from, double *to, uint64_t N) {
    for (long long ti = 0; ti < N / M; ++ti)
    { // assuming N=M*x
        double sum = 0;
        for (long long fi = ti * M; fi < (ti + 1) * M; ++fi)
            sum += from[fi];
        to[ti] = sum / M;
    }
}

int sock_r = -1;

int main(int argc, char *argv[])
{
    //root-only data, here because why not
    struct sockaddr_in sa_srv;
    uint64_t avg_len;
    double *vect, *avg_vect;
    double start, end; // timer
    double time_used;
    char msg[BUFSIZE];

    //process data
    const int root = 0;
    int my_rank; // My process rank
    int num_procs; //Number of processes
    //similar data
    uint64_t len;

    //init stuff
    MPI_Init(&argc, &argv);//MPI Initialize
    MPI_Comm_rank(MPI_COMM_WORLD,&my_rank);// Получить текущий номер процесса
    MPI_Comm_size(MPI_COMM_WORLD,&num_procs);

    //Step 1: root gets data and sends size OR error response, everyone waits.
    if (my_rank == root)
    {
        if (argc != 3)
        {
            fprintf(stderr, "Usage: %s <server_address/hostname> <server_port>\n", argv[0]);
            len = 0;
            MPI_Bcast(&len, 1, MPI_UNSIGNED_LONG, root, MPI_COMM_WORLD);
            return -1;
        }
        sa_srv.sin_family = AF_INET;
        if (inet_pton(AF_INET, argv[1], &sa_srv.sin_addr))
            logwrite("Address is a valid IP\n");
        else
        {
            logwrite("Address is a hostname... maybe\n");
            sa_srv.sin_addr = lookup_host(argv[1]);
        }
        sa_srv.sin_port = htons(atoi(argv[2]));
        logwrite("Connecting...\n");
        sock_r = socket(PF_INET, SOCK_STREAM, 0);
        sa_srv.sin_family = AF_INET;
        //check error <=0
        if (connect(sock_r, (struct sockaddr *)&sa_srv, sizeof(sa_srv)) == -1) {
            perror("Error while connecting");
            len = 0;
            MPI_Bcast(&len, 1, MPI_UNSIGNED_LONG, root, MPI_COMM_WORLD);
            return -1;
        }
        //read the data
        len = sock_rcv(sock_r, &vect);
        avg_len = len / M;
        avg_vect = calloc(avg_len, sizeof(double));
        logwrite("Sending data to forks and calculating...");
        start = MPI_Wtime(); //start timing
        //send size to people
        MPI_Bcast(&len, 1, MPI_UNSIGNED_LONG, root, MPI_COMM_WORLD);
        //logwrite("Broadcasting my trash...");
    }
    else
    {
        //get size from root
        MPI_Bcast(&len, 1, MPI_UNSIGNED_LONG, root, MPI_COMM_WORLD);
        //logwrite("Broadcasting NOT my trash...");
        if (len == 0)
            return -1;
    }

    //Step 2: init sizes and send data
    //Количество данных для каждого процесса
    uint64_t blocks_per_process = (len / M) / num_procs; //items per process (rounded down!)
    uint64_t extra_blocks = (len / M) % num_procs; //extra items to count
    //Local process data
    uint64_t local_blocks = my_rank < extra_blocks ? blocks_per_process + 1 : blocks_per_process;
    //Send stuff
    if (my_rank == root) {
        //logwrite("Sending my trash...");
        //send to others
        for (int i = 1; i < num_procs; ++i) {
        //logwrite_int("Sending my trash to: ", i);
            if (i < extra_blocks)
                MPI_Send(vect + (blocks_per_process + 1) * M * i, (blocks_per_process + 1) * M, MPI_DOUBLE, i, 0, MPI_COMM_WORLD);
            else
                MPI_Send(vect + blocks_per_process * M * i + extra_blocks, blocks_per_process * M, MPI_DOUBLE, i, 0, MPI_COMM_WORLD);
        }
        // use the start of the vector myself!
    }
    else
    {
        //logwrite("Getting my trash...");
        //allocate
        vect = (double *)malloc(local_blocks * M * sizeof(double));
        avg_vect = (double *)malloc(local_blocks * sizeof(double));
        //get data
        MPI_Recv(vect, local_blocks * M, MPI_DOUBLE, root, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        //logwrite_int("Received my trash: ", my_rank);
    }

    //Step 3: calculate local data. For root - only starting part!
    //logwrite("Calcing my trash...");
    AVG(vect, avg_vect, local_blocks * M);

    //Step 4: gather and send back
    if (my_rank == root) {
        //logwrite("Waiting my trash...");
        //get from each
        for (int i = 1; i < num_procs; ++i) {
            if (i < extra_blocks)
                MPI_Recv(avg_vect + (blocks_per_process + 1) * i, (blocks_per_process + 1), MPI_DOUBLE, i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            else
                MPI_Recv(avg_vect + blocks_per_process * i + extra_blocks, blocks_per_process, MPI_DOUBLE, i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
        end = MPI_Wtime(); //stop timing
        time_used = end - start;
        printf("Time used: %lf.\n", time_used);
        //send back
        logwrite("Finished, sending...");
        sock_send(sock_r, avg_vect, avg_len);
        sock_rcv_str(sock_r, msg); //get ACK. Stupid.
        sprintf(msg, "Time used: %lf. Total data size: %lf MB. N = %ld, M = %ld", time_used, (double)len * sizeof(double) / 1024 / 1024, len, M);
        sock_send_str(sock_r, msg);
        shutdown(sock_r, SHUT_WR);
    }
    else
    {
        //logwrite("Sendingback my trash...");
        MPI_Send(avg_vect, local_blocks, MPI_DOUBLE, root, 1, MPI_COMM_WORLD);
    }
    //logwrite_int("Done: ", my_rank);

    //Step 5: free memory and return
    free(vect);
    free(avg_vect);
    MPI_Finalize();//Конец
    return 0;
}