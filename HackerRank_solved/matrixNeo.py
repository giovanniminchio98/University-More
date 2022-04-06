import os 
import numpy
import re
'''
EXPLANATION ROW # 33
First: ([A-Za-z0-9]), the "()" capture the match as a group (useful to reuse the match later) 
    and [A-Za-z0-9] means any character alphanumeric.
Second: [!@#$%&\s]+ means match any symbol between square braces 
    (the \s refers to any spacing character) one or more times.
Third: match any alphanumeric character but not consume strings. 
    This means, when the pattern match the position for nexts matches do not take this into account.
'''

'''
I had troouble in ther solution, since i had no idea aboiut how to write row # 33
The rest was pretty basic...
'''
if __name__ == '__main__':
    n = input()
    m=input()
    matrix = []
    dictio = {}
    special = ['!','@','#','$','%','&']
    for _ in range(n):
        matrix_item = input()
        for i in range(len(matrix_item)):
            try:
                dictio[i] += str(matrix_item[i])
            except:
                dictio[i] = str(matrix_item[i])
            
    text = ''.join([elem for elem in dictio.values()])
    pattern = r'([A-Za-z0-9])[!@#$%&\s]+(?=[A-Za-z0-9])'
    text = re.sub(pattern,r'\1 ', text)
    print(text)
