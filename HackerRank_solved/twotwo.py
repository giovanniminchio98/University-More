import os
import numpy as np
import math


' my python solution, NOT optimal --> points : 38.89/150 '
def twoTwo_mine(a):
    # Write your code here
    
    cnt=0
    for i in range(801):
        num=2**i
        dim = len(str(num))
        if dim > len(a):
            break
        flag = str(num) in a
        k=dim
        
        cnt += a.count(str(num))

    return cnt

' same solution as mine, but in javascript --> points : 150/150 '
# # function twoTwo(a) {
# #     let res = 0, str = '';

# #     for(let i = 0; i<=800; i++) {
# #         str = BigInt(2**i).toString();
# #         let pos = 0;
# #         while((pos = a.indexOf(str, pos)) > -1) {
# #             res++;
# #             pos++;
# #         }
# #     }
# #     return res;
# # }
' since it uses Bigint is run in accepted time by HackerRank, while python did not'


' optimal solution would involve the use of Tries, i will try... '
# coming...




if __name__ == '__main__':
    print('start')
    twoTwo_mine(47584843837)