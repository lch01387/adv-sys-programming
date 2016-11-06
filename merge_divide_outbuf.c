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


// each input file size is 100M = 104857600byte
#define FILE_SIZE 104857600

int readaline_and_out(char* buf, char* buf_out, int* cursor, int* cursor_out);
void set_cursor(char* buf, int* line_in, int* cursor_in);
void reverse(char* str, int count);
void reverse_remain(char *buf_in, char *buf_out);
int get_line_num(char *buf);
void swap(char *a, char *b);

char buf_in[2][FILE_SIZE];
char buf_remain[FILE_SIZE];

int main(int argc, char *argv[])
{
    int file1, file2, fout;
    int thread_num; // number of max thread(core)
    int line_num[2], common_line;
    int remain_limit = 0, remain_line = 0;
    int cursor_remain_in = 0, cursor_remain_out = 0;
    int *limit, *temp_line;
    int *line_in, **cursor_in;
    int *cursor_out;
    char **buf_out;
    
    /* calculate time, etc */
    struct timeval before, after;
    int eof = 0;
    int duration;
    int ret = 1;
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
#pragma omp parallel sections num_threads(1) // thread갯수 늘렸을때 이유를 알수없는 버그.. thread 1개로 설정
    {
#pragma omp section
        {
            read(file1, buf_in[0], FILE_SIZE);
            line_num[0] = get_line_num(buf_in[0]); // file1. total line number
        }
#pragma omp section
        {
            read(file2, buf_in[1], FILE_SIZE);
            line_num[1] = get_line_num(buf_in[1]); // file2. total line number
        }
    }
    
    thread_num = omp_get_num_procs(); // get max thread number
    if(thread_num == 0){
        perror("error when get thread_num");
        goto leave3;
    }
    
    /* dynamic allocation */
    /*--------------------------------------------------------------------*/
    cursor_out = (int*)calloc(thread_num, sizeof(int*));
    if(cursor_out == NULL) goto leave4;
    buf_out = (char**)malloc(sizeof(char*)*thread_num);
    if(cursor_out == NULL) goto leave5;
    for(i=0;i<thread_num;i++){
        buf_out[i] = (char*)malloc(sizeof(char)*FILE_SIZE*2);
        if(buf_out[i] == NULL) goto leave6;
    }
    line_in = (int*)calloc(thread_num, sizeof(int));
    if(line_in == NULL) goto leave7;
    cursor_in = (int**)calloc(2, sizeof(int*));
    if(cursor_in == NULL) goto leave8;
    for(i=0;i<2;i++){
        cursor_in[i] = (int*)calloc(thread_num, sizeof(int));
        if(cursor_in[i]==NULL) goto leave9;
    }
    limit = (int*)calloc(thread_num, sizeof(int));
    if(limit == NULL) goto leave10;
    temp_line = (int*)calloc(thread_num, sizeof(int));
    if(temp_line == NULL) goto leave11;
    /*--------------------------------------------------------------------*/
    
    if(line_num[0] > line_num[1])
        common_line = line_num[1];
    else
        common_line = line_num[0];
    
    /* calculate start line number for each thread */
    for(i=1;i<thread_num; i++){
        line_in[i] = line_in[i-1] + common_line/thread_num;
        line_in[i] = line_in[i-1] + common_line/thread_num;
    }
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
    
    /* this pragma help each loop run in parallel. */
#pragma omp parallel for num_threads(thread_num)
    for(i=0; i<thread_num; i++){
        if(i==thread_num-1)
            limit[i] = common_line;
        else
            limit[i] = line_in[i+1];
        temp_line[i] = line_in[i];
        do {
            eof = readaline_and_out(buf_in[0], buf_out[i], &cursor_in[0][i], &cursor_out[i]);
            eof = readaline_and_out(buf_in[1], buf_out[i], &cursor_in[1][i], &cursor_out[i]);
            if(eof) break; // cannot use branch in parallel (return, goto)
            temp_line[i]++;
        } while (temp_line[i] < limit[i]);
    }
    if(eof){
        perror("meet cursor error\n");
        goto leave0;
    }
    
    /* file write. no parallel */
    for(i=0; i<thread_num; i++)
        write(fout, buf_out[i], cursor_out[i]);
    
    /* write remain buffer */
    if(line_num[0]-line_num[1] > 0){
        remain_limit = line_num[0] - line_num[1];
        do{
            readaline_and_out(buf_in[0]+cursor_in[0][thread_num-1], buf_remain, &cursor_remain_in, &cursor_remain_out);
            remain_line++;
        } while (remain_line < remain_limit);
        write(fout, buf_remain, FILE_SIZE-cursor_in[0][thread_num-1]);
    }else if(line_num[0]-line_num[1] < 0){
        remain_limit = line_num[1] - line_num[0];
        do{
            readaline_and_out(buf_in[1]+cursor_in[1][thread_num-1], buf_remain, &cursor_remain_in, &cursor_remain_out);
            remain_line++;
        } while (remain_line < remain_limit);
        write(fout, buf_remain, FILE_SIZE-cursor_in[1][thread_num-1]);
    }
    // if equal, do nothing
    
    gettimeofday(&after, NULL);
    
    duration = (after.tv_sec - before.tv_sec) * 1000000 + (after.tv_usec - before.tv_usec);
    printf("Processing time = %d.%06d sec\n", duration / 1000000, duration % 1000000);
    printf("File1 = %ld, File2= %ld, Total = %ld Lines\n", line_num[0], line_num[1], line_num[0]+line_num[1]);
    ret = 0;
    
leave11:
    free(temp_line);
leave10:
    free(limit);
leave9:
    for(i=0;i<2;i++)
        free(cursor_in[i]);
leave8:
    free(line_in);
leave7:
    free(cursor_in);
leave6:
    for(i=0;i<thread_num;i++)
        free(buf_out[i]);
leave5:
    free(buf_out);
leave4:
    free(cursor_out);
leave3:
    close(fout);
leave2:
    close(file2);
leave1:
    close(file1);
leave0:
    return ret;
}

int get_line_num(char *buf){
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

void set_cursor(char *buf, int *line_in, int *cursor_in){
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

void reverse(char* str, int count){
    int i=0; int j=count-1;
    char tmp;
    while(i<j){
        tmp = str[i];
        str[i] = str[j];
        str[j] = tmp;
        i++; j--;
    }
}
