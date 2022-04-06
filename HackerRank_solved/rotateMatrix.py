import math
import os
import random
import re
import sys

' not working yet. Passes only 4 cases / 12'
' problems due to special cases not handles'

' ACTUALLY WOULD BE BETTER TO JUST MAP POSITIONS OF THE MATRIX, ROTATE THEM AND THEN SWAP ELEMENTS'

' so the part where i save all the elements in an array is not mandatory'

#
# Complete the 'matrixRotation' function below.
#
# The function accepts following parameters:
#  1. 2D_INTEGER_ARRAY matrix
#  2. INTEGER r
#
def matrixRotation(matrix, r):
    row = len(matrix)
    cols = len(matrix[0])
    
    circles=[]
    
    start = [0,0]
    end = [0,0]
    
    while row > 0 and cols > 0:
        circle = []
        
        circle.append(matrix[end[0]][end[1]])
        # down
        cnt=0
        flag=True
        while cnt<row-1 and flag:
            end=[end[0]+1, end[1]]
            circle.append(matrix[end[0]][end[1]])
            cnt+=1
            flag=True
            
        # right
        cnt=0

        while cnt<cols-1 and flag:
            end=[end[0], end[1]+1]
            circle.append(matrix[end[0]][end[1]])
            cnt+=1
            flag=True
            
        # up
        cnt=0
        while cnt<row-1 and flag:
            end=[end[0]-1, end[1]]
            circle.append(matrix[end[0]][end[1]])
            cnt+=1
            false=True
            
        # left
        cnt=0
        while cnt<cols-2 and flag:
            end=[end[0], end[1]-1]
            circle.append(matrix[end[0]][end[1]])
            cnt+=1
            flag=True
            
            
        circles.append(circle)
            
        row-=2
        cols-=2

        end = [start[0]+1, start[1]+1]
        
    # for rot in range(r):
    for i in range(len(circles)):
        lst = circles[i]
        for rot in range(r):
            lst.insert(0,lst.pop(-1))
        circles[i] = lst
            
    # change the matrix values
    row = len(matrix)
    cols = len(matrix[0])
    start = [0,0]
    end = [0,0]
    
    count=0
    while row > 0 and cols > 0:
        circle = circles[count]
        count+=1
        
        matrix[end[0]][end[1]] = circle.pop(0)
        # down
        cnt=0
        flag=True
        while cnt<row-1 and flag:
            end=[end[0]+1, end[1]]
            # circle.append(matrix[end[0]][end[1]])
            matrix[end[0]][end[1]] = circle.pop(0)
            cnt+=1
            flag=True
            
        # right
        cnt=0
        while cnt<cols-1 and flag:
            end=[end[0], end[1]+1]
            # circle.append(matrix[end[0]][end[1]])
            matrix[end[0]][end[1]] = circle.pop(0)
            cnt+=1
            flag=True
            
        # up
        cnt=0
        while cnt<row-1 and flag:
            end=[end[0]-1, end[1]]
            # circle.append(matrix[end[0]][end[1]])
            matrix[end[0]][end[1]] = circle.pop(0)
            cnt+=1
            flag=True
            
        # left
        cnt=0
        while cnt<cols-2 and flag:
            end=[end[0], end[1]-1]
            # circle.append(matrix[end[0]][end[1]])
            matrix[end[0]][end[1]] = circle.pop(0)
            cnt+=1
            flag=True
            
        # circles.append(circle)
            
        row-=2
        cols-=2
        end = [start[0]+1, start[1]+1]
        
    for i in range(len(matrix)):
        for j in range(len(matrix[0])):
            print(matrix[i][j], end=' ')
        print('', end='\n')
    
            
        
        
        
        

if __name__ == '__main__':
    # first_multiple_input = input().rstrip().split()

    # m = int(first_multiple_input[0])

    # n = int(first_multiple_input[1])

    # r = int(first_multiple_input[2])

    matrix = [[1, 2, 3, 4], [5 ,6 ,7, 8], [9 ,10, 11, 12], [13 ,14 ,15, 16]]

    # for _ in range(m):
    #     matrix.append(list(map(int, input().rstrip().split())))
    r=2
    matrixRotation(matrix, r)
