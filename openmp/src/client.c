#include "../../common/lib.h"
#include "omp.h"

const uint64_t M = 8192;

void AVG(const double *from, double *to, uint64_t N) {
    //printf("N %ld, M %ld\n", N, M);
    double sum = 0;
    #pragma omp parallel private(sum)
    {
        #pragma omp for
        for (long long ti = 0; ti < N / M; ++ti)
        { // assuming N=M*x
            sum = 0;
            for (long long fi = ti * M; fi < (ti + 1) * M; ++fi)
                sum += from[fi];
            to[ti] = sum / M;
        }
    }
}

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

int sock_r = -1;

int main(int argc, char *argv[])
{    
    int ret;
    struct sockaddr_in sa_srv;
    uint64_t len, avg_len;
    double *vect, *avg_vect;
    double start, end;
    double time_used;
    char msg[BUFSIZE];

    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s <server_address/hostname> <server_port>\n", argv[0]);
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
    CHECK(connect(sock_r, (struct sockaddr *)&sa_srv, sizeof(sa_srv)), -1, "Error while connecting")
    //read the data
    len = sock_rcv(sock_r, &vect);
    avg_len = len / M;
    avg_vect = calloc(avg_len, sizeof(double));
    logwrite("Got data, calculating...");
    start = omp_get_wtime();
    AVG(vect, avg_vect, len);
    end = omp_get_wtime();
    time_used = end - start;
    printf("Time used: %lf.\n", time_used);
    logwrite("Finished, sending...");
    sock_send(sock_r, avg_vect, avg_len);
    sock_rcv_str(sock_r, msg); //get ACK. Stupid.
    sprintf(msg, "Time used: %lf. Total data size: %lf MB. N = %ld, M = %ld", time_used, (double)len * sizeof(double) / 1024 / 1024, len, M);
    sock_send_str(sock_r, msg);
    shutdown(sock_r, SHUT_WR);
    free(vect);
    free(avg_vect);
    return 0;
}