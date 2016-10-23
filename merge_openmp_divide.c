#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

int readaline_and_out(char* buf, char* buf_out, int* cursor, int* cursor_out);
void reverse(char* str, int count);
void swap(char *a, char *b);
char buf1[104857600];
char buf2[104857600];
char buf_out1[209715199];
char buf_out2[209715199];


int main(int argc, char *argv[])
{
    int file1, file2, fout;
    int cursor1=0, cursor2=0;
    int cursor_out1=0, cursor_out2=0;
    int limit;
    
    int eof1 = 0, eof2 = 0;
    long line1 = 0, line2 = 0, lineout = 0;
    struct timeval before, after;
    int duration;
    int ret = 1;

    if (argc != 4) {
        fprintf(stderr, "usage: %s file1 file2 fout\n", argv[0]);
        goto leave0;
    }
    if ((file1 = open(argv[1], O_RDONLY)) == -1) {
        perror(argv[1]);
        goto leave0;
    }
    if ((file2 = open(argv[2], O_RDONLY)) == -1) {
        perror(argv[2]);
        goto leave1;
    }
    if ((fout = open(argv[3], O_WRONLY | O_CREAT | O_APPEND, 0644)) == -1) {
        perror(argv[3]);
        goto leave2;
    }
    
    gettimeofday(&before, NULL);
#pragma omp parallel
{
#pragma omp sections
    {
#pragma omp section
    read(file1, buf1, 104857600);
#pragma omp section
    read(file2, buf2, 104857600);
    }
}
    // 0~104857600
#pragma omp parallel num_threads(2)
{
#pragma omp sections
    {
#pragma omp section
        {
    limit = 104857600;
    do {
        if (!eof1) {
            if (!readaline_and_out(buf1, buf_out1, &cursor1, &cursor_out1)) {
                line1++; lineout++;
            } else
                eof1 = 1;
        }
        if (!eof2) {
            if (!readaline_and_out(buf2, buf_out1, &cursor2, &cursor_out1)) {
                line2++; lineout++;
            } else
                eof2 = 1;
        }
    } while ((!eof1 || !eof2) && cursor_out1 < limit);
        }
    // 104857600~209715200
#pragma omp section
        {
    limit = 209715199;
    do {
        if (!eof1) {
            if (!readaline_and_out(buf1, buf_out2, &cursor1, &cursor_out2)) {
                line1++; lineout++;
            } else
                eof1 = 1;
        }
        if (!eof2) {
            if (!readaline_and_out(buf2, buf_out2, &cursor2, &cursor_out2)) {
                line2++; lineout++;
            } else
                eof2 = 1;
        }
    } while ((!eof1 || !eof2) && cursor_out2 < limit);
        }
    }
}
    buf_out2[209715199] = '\0';
    printf("size: %d\n", strlen(buf_out1));
    printf("size: %d\n", strlen(buf_out2));
    write(fout, buf_out1, limit);
    write(fout, buf_out2, limit);
    gettimeofday(&after, NULL);
    
    duration = (after.tv_sec - before.tv_sec) * 1000000 + (after.tv_usec - before.tv_usec);
    printf("Processing time = %d.%06d sec\n", duration / 1000000, duration % 1000000);
    printf("File1 = %ld, File2= %ld, Total = %ld Lines\n", line1, line2, lineout);
    ret = 0;
leave3:
    close(fout);
leave2:
    close(file2);
leave1:
    close(file1);
leave0:
    return ret; 
}

int readaline_and_out(char* buf, char* buf_out, int* cursor, int* cursor_out)
{
    int ch, count = 0;
    int i;
    char str[100];
    do {
        if ((ch = buf[*cursor]) == '\0' || *cursor >= 104857600) {
            if (!count)
                return 1;
            else {
                break;
            }
        }
        str[count] = ch;
        count++;
        *cursor+=1;
    } while (ch != '\n');
    str[count] = '\0';
    reverse(str,count-1);
    for(i=0;i<count;i++)
        buf_out[*cursor_out+i] = str[i];
    *cursor_out+=count;
    return 0;
}

void reverse(char* str, int count){
    char temp, i;
#pragma omp parallel
    {
#pragma omp for
        for(i=0; i<count/2; i++){
            swap(&str[i], &str[count-1-i]);
        }
    }
}

void swap(char *a, char *b){
    int temp = *a;
    *a = *b;
    *b = temp;
}
