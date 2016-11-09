/****************************************
 
 * 3학년 20113313 이창현
 
 * optimizing merge program
 
 ****************************************/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>
#include <fcntl.h>
#include <omp.h>
#include <sys/stat.h>

/* swap macro from qsort.c Written by Douglas C. Schmidt */
#define SWAP(a, b)						      \
do									      \
{									      \
    register char *__a = (a), *__b = (b);				      \
    char __tmp = *__a;						      \
    *__a++ = *__b;						      \
    *__b++ = __tmp;									      \
} while (0)

int readaline_and_out(char* buf, char* buf_out, int* cursor, int* cursor_out);
void set_cursor(char* buf, int* line_in, int* cursor_in);
void reverse(char* str, int count);
int get_line_num(char *buf);

int main(int argc, char *argv[])
{
    int file1, file2, fout;
    int thread_num, big;
    int file_size[2], line_num[2], common_line;
    int remain_limit = 0, remain_line = 0;
    int cursor_remain_in = 0, cursor_remain_out = 0;
    int *limit, *temp_line;
    int *line_in, **cursor_in, *cursor_out;
    char **buf_in, **buf_out;
    char *buf_remain;
    
    /* calculate time, etc */
    struct timeval before, after;
    struct stat st1, st2;
    int error = 0, ret = 1;
    int duration;
    int i;
    
    if (argc != 4) {
        fprintf(stderr, "usage: %s file1 file2 fout\n", argv[0]);
        goto leave0;
    }
    if ((file1 = open(argv[1], O_RDONLY)) == -1) {
        perror(argv[1]);
        goto leave1;
    }
    if ((file2 = open(argv[2], O_RDONLY)) == -1) {
        perror(argv[2]);
        goto leave2;
    }
    if ((fout = open(argv[3], O_WRONLY | O_CREAT, 0644)) == -1) {
        perror(argv[3]);
        goto leave3;
    }
    
    gettimeofday(&before, NULL);
    
    /* get file size */
    if((fstat(file1, &st1) == -1) || (fstat(file2, &st2) == -1)){
        perror("fstat error");
        goto leave3;
    }
    file_size[0] = st1.st_size;
    file_size[1] = st2.st_size;
    printf("file1_size: %d, file2_size: %d\n", file_size[0], file_size[1]);
    
    /* get max thread number */
    thread_num = omp_get_num_procs();
    if(thread_num == 0){
        perror("error when get thread_num");
        goto leave3;
    }
    printf("%d-thread parallization\n", thread_num);
    
    /* dynamic allocation */
    {
        buf_remain = (char*)malloc(sizeof(char)*(file_size[0]+file_size[1]));
        if(buf_remain == NULL) goto leave4;
        cursor_out = (int*)calloc(thread_num, sizeof(int*));
        if(cursor_out == NULL) goto leave4;
        line_in = (int*)calloc(thread_num, sizeof(int));
        if(line_in == NULL) goto leave4;
        limit = (int*)calloc(thread_num, sizeof(int));
        if(limit == NULL) goto leave4;
        temp_line = (int*)calloc(thread_num, sizeof(int));
        if(temp_line == NULL) goto leave4;
        
        buf_in = (char**)malloc(sizeof(char*)*2);
        if(buf_in == NULL) goto leave5;
        for(i=0;i<2;i++){
            buf_in[i] = (char*)malloc(sizeof(char)*(file_size[i]+1)); // prevent bus error in parallel(+1)
            if(buf_in[i] == NULL) goto leave6;
        }
        buf_out = (char**)malloc(sizeof(char*)*thread_num);
        if(buf_out == NULL) goto leave7;
        for(i=0;i<thread_num;i++){
            buf_out[i] = (char*)malloc(sizeof(char)*(file_size[0]+file_size[1]));
            if(buf_out[i] == NULL) goto leave8;
        }
        cursor_in = (int**)calloc(2, sizeof(int*));
        if(cursor_in == NULL) goto leave9;
        for(i=0;i<2;i++){
            cursor_in[i] = (int*)calloc(thread_num, sizeof(int));
            if(cursor_in[i] == NULL) goto leave10;
        }
    }
    
#pragma omp parallel sections
    {
#pragma omp section
        {
            if(read(file1, buf_in[0], file_size[0]) == -1) error++;
            line_num[0] = get_line_num(buf_in[0]); // file1. total line number
        }
#pragma omp section
        {
            if(read(file2, buf_in[1], file_size[1]) == -1) error++;
            line_num[1] = get_line_num(buf_in[1]); // file2. total line number
        }
    }
    if(error){
        perror("file read error");
        goto leave10;
    } // cannot use branch(goto, return) in parallel section
    
    if(line_num[0] > line_num[1])
        common_line = line_num[1];
    else
        common_line = line_num[0];
    
    /* calculate start line number for each thread */
    for(i=1;i<thread_num; i++)
        line_in[i] = line_in[i-1] + common_line/thread_num;
    
    /* set start cursor for each thread */
#pragma omp parallel sections
    {
#pragma omp section
        {
            set_cursor(buf_in[0], line_in, cursor_in[0]);
        }
#pragma omp section
        {
            set_cursor(buf_in[1], line_in, cursor_in[1]);
        }
    }
    
    /* read input buffer line by line and write reverse string on output buffer */
    /* this pragma help each loop run in parallel. */
#pragma omp parallel for num_threads(thread_num)
    for(i=0; i<thread_num; i++){
        if(i == thread_num-1)
            limit[i] = common_line;
        else
            limit[i] = line_in[i+1];
        temp_line[i] = line_in[i];
        do {
            error = readaline_and_out(buf_in[0], buf_out[i], &cursor_in[0][i], &cursor_out[i]);
            error = readaline_and_out(buf_in[1], buf_out[i], &cursor_in[1][i], &cursor_out[i]);
            if(error) break; // cannot use branch in parallel (return, goto)
            temp_line[i]++;
        } while (temp_line[i] < limit[i]);
    }
    if(error){
        perror("meet cursor error\n");
        goto leave10;
    }
    
    /* file write. no parallel */
    for(i=0; i<thread_num; i++)
        write(fout, buf_out[i], cursor_out[i]);
    
    /* write remain buffer */
    if(line_num[0]-line_num[1] > 0){
        remain_limit = line_num[0] - line_num[1];
        big = 0;
    }else{
        remain_limit = line_num[1] - line_num[0];
        big = 1;
    }
    do{
        readaline_and_out(buf_in[big]+cursor_in[big][thread_num-1], buf_remain, &cursor_remain_in, &cursor_remain_out);
        remain_line++;
    } while (remain_line < remain_limit);
    write(fout, buf_remain, file_size[big]-cursor_in[big][thread_num-1]);
    
    gettimeofday(&after, NULL);
    
    duration = (after.tv_sec - before.tv_sec) * 1000000 + (after.tv_usec - before.tv_usec);
    printf("Processing time = %d.%06d sec\n", duration / 1000000, duration % 1000000);
    printf("File1 = %ld, File2= %ld, Total = %ld Lines\n", line_num[0], line_num[1], line_num[0]+line_num[1]);
    ret = 0;
    
leave10:
    for(i=0;i<2;i++)
        free(cursor_in[i]);
leave9:
    free(cursor_in);
leave8:
    for(i=0;i<thread_num;i++)
        free(buf_out[i]);
leave7:
    free(buf_out);
leave6:
    for(i=0;i<2;i++)
        free(buf_in[i]);
leave5:
    free(buf_in);
leave4:
    free(cursor_out);
    free(buf_remain);
    free(line_in);
    free(limit);
    free(temp_line);
leave3:
    close(fout);
leave2:
    close(file2);
leave1:
    close(file1);
leave0:
    return ret;

}

int get_line_num(char *buf)
{
    int cursor = 0;
    int line_num = 0;
    while(buf[cursor] != '\0'){
        if(buf[cursor]=='\n'){
            line_num++;
        }
        cursor++;
    }
    return line_num;
}

void set_cursor(char *buf, int *line_in, int *cursor_in)
{
    int cursor = 0;
    int line_num = 0;
    int index=1;
    while(buf[cursor] != '\0'){
        if(buf[cursor] == '\n'){
            line_num++;
            if(line_num == line_in[index]){
                cursor_in[index] = cursor+1;
                index++;
            }
        }
        cursor++;
    }
}

int readaline_and_out(char *buf_in, char *buf_out, int *cursor_in, int *cursor_out)
{
    int ch, count = 0;
    int i;
    char str[1000];
    int temp_line = 0;
    
    if(buf_in[*cursor_in] == '\0' || buf_in[*cursor_in] == '\n')
        return 1;
    
    do{
        ch = buf_in[*cursor_in+count];
        str[count] = ch;
        count++;
    }while(ch != '\n' && ch!='\0');
    
    reverse(str, count-1);
    for(i=0;i<=count;i++)
        buf_out[*cursor_out+i] = str[i];
    
    *cursor_in += count;
    *cursor_out += count;
    return 0;
}

void reverse(char* str, int count)
{
    int i=0; int j=count-1;
    while(i<j){
        SWAP(str+i, str+j);
        i++; j--;
    }
}
