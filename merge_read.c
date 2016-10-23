#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

int readaline_and_out(char* buf, FILE* fout, int* cursor);
char buf1[104857600];
char buf2[104857600];
//char buf_out[209715200];


int main(int argc, char *argv[])
{   
    int file1, file2;
    FILE *fout;
    int cursor1=0, cursor2=0;
    
    int eof1 = 0, eof2 = 0;
    long line1 = 0, line2 = 0, lineout = 0;
    struct timeval before, after;
    int duration;
    int ret = 1;

    if (argc != 4) {
        fprintf(stderr, "usage: %s file1 file2 fout\n", argv[0]);
        goto leave0;
    }
    if ((file1 = open(argv[1], O_RDONLY | O_CREAT)) == -1) {
        perror(argv[1]);
        goto leave0;
    }
    if ((file2 = open(argv[2], O_RDONLY | O_CREAT)) == -1) {
        perror(argv[2]);
        goto leave1;
    }
    if ((fout = fopen(argv[3], "wt")) == NULL) {
        perror(argv[3]);
        goto leave2;
    }
    
    gettimeofday(&before, NULL);
    read(file1, buf1, 104857600);
    read(file2, buf2, 104857600);
    do {
        if (!eof1) {
            if (!readaline_and_out(buf1, fout, &cursor1)) {
                line1++; lineout++;
            } else
                eof1 = 1;
        }
        if (!eof2) {
            if (!readaline_and_out(buf2, fout, &cursor2)) {
                line2++; lineout++;
            } else
                eof2 = 1;
        }
    } while (!eof1 || !eof2);
    gettimeofday(&after, NULL);
    
    duration = (after.tv_sec - before.tv_sec) * 1000000 + (after.tv_usec - before.tv_usec);
    printf("Processing time = %d.%06d sec\n", duration / 1000000, duration % 1000000);
    printf("File1 = %ld, File2= %ld, Total = %ld Lines\n", line1, line2, lineout);
    ret = 0;
    
leave3:
    fclose(fout);
leave2:
    close(file2);
leave1:
    close(file1);
leave0:
    return ret; 
}

int readaline_and_out(char* buf, FILE* fout, int* cursor)
{
    int i, j, temp, ch, count = 0;
    char str[100];
    do {
        if ((ch = buf[*cursor]) == '\0') {
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
    i = 0; j = count-2;
    while(i<j){
        temp = str[i];
        str[i] = str[j];
        str[j] = temp;
        i++; j--;
    }
    fputs(str, fout);
    return 0;
}

