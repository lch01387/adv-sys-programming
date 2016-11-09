# 고급 시스템 프로그래밍 Advanced System Programming

국민대학교 컴퓨터공학부

- 20113313 이창현
- 두 파일을 한줄씩 읽어 새로운 파일에 번갈아쓰는 merge프로그램을 최적화
 
File

- gen.c   : n 개의  m MB의 크기의 text 파일 만들기 (file merge용)

- merge_origin.c : 원본 merge코드

- merge_final.c : 최종 optimized된 merge 코드

성능개선 최종결과: 119.83초 => 30.40초 ( 31.84 / 119.84* 100) = 26.5%
