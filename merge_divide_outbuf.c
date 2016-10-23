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
char buf_out1[104857700];
char buf_out2[104857700];


int main(int argc, char *argv[])
{
    int file1, file2, fout;
    int cursor1_1=0, cursor2_1=0;
    int cursor1_2=52428800, cursor2_2=52428800;
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
    if ((fout = open(argv[3], O_WRONLY | O_CREAT, 0644)) == -1) {
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
#pragma omp parallel num_threads(2)
    {
#pragma omp sections
        {
#pragma omp section
            {
                limit = 104857600;
                do {
                    if (!eof1) {
                        if (!readaline_and_out(buf1, buf_out1, &cursor1_1, &cursor_out1)) {
                            line1++; lineout++;
                        } else
                            eof1 = 1;
                    }
                    if (!eof2) {
                        if (!readaline_and_out(buf2, buf_out1, &cursor2_1, &cursor_out1)) {
                            line2++; lineout++;
                        } else
                            eof2 = 1;
                    }
                } while ((!eof1 || !eof2) && cursor_out1 <= limit);
            }
#pragma omp section
            {
                limit = 104857600;
                do {
                    if (!eof1) {
                        if (!readaline_and_out(buf1, buf_out2, &cursor1_2, &cursor_out2)) {
                            line1++; lineout++;
                        } else
                            eof1 = 1;
                    }
                    if (!eof2) {
                        if (!readaline_and_out(buf2, buf_out2, &cursor2_2, &cursor_out2)) {
                            line2++; lineout++;
                        } else
                            eof2 = 1;
                    }
                } while ((!eof1 || !eof2) && cursor_out2 <= limit);
            }
        }
    }
    write(fout, buf_out1, 104857600);
    write(fout, buf_out2, 104857600);
    
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
int readaline_and_out(char* buf_in, char* buf_out, int* cursor, int* cursor_out)
{
    int ch, count = 0;
    int i;
    char str[1000];
    
    if(buf_in[*cursor] == '\0' || *cursor > 104857199){
        return 1;
    }
    count = 0;
    while((ch=buf_in[*cursor+count]) != '\n'){
        str[count] = ch;
        count++;
    }
    str[count] = '\n';
    reverse(str, count);
    for(i=0;i<=count;i++){
        buf_out[*cursor_out+i] = str[i];
    }
    *cursor += count+1;
    *cursor_out += count+1;
    return 0;
}

void reverse(char* str, int count){
    char temp, i;
    
    for(i=0; i<count/2; i++){
        swap(&str[i], &str[count-1-i]);
    }
}

void swap(char *a, char *b){
    int temp = *a;
    *a = *b;
    *b = temp;
}
