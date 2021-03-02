# Calculate query keys with the following tuples {key,offset}
def cython_calculateQueryKeysDesplR(offset, size, lines, lines_size, keySize, queryLength):
    KeysDespl = []
    
    #tg1 = time()
    # Calculate the first and last keys desplazaments.
    first_key = 0
    if (size!=lines_size):    
        # Internal lines.
        last_key = size
    else:
        # Last file line.
        last_key = size - keySize + 1
        
    offset = queryLength-(offset+first_key+1)
    for k in range (first_key, last_key):       
        # Add key to python list
        KeysDespl.append((lines[k:k+keySize],int(offset)))
        offset -= 1
        
    #print("\rProcessing {} keys from offset {} in {} secs".format(len(Keys),offset, round(time() - gt0.value,3), end =" "))
    
    return KeysDespl
  
    
# Calculate multiple query keys with the following tuples {key,offset}
def cython_calculateMultipleQueryKeysDesplR(query, size, keySize):    
    cdef int first_key, last_key, offset, k
    cdef list KeysDespl = []
    #print("cython_calculateMultipleQueryKeysDesplR {} {} {}".format(query, size, keySize))
  
    # Calculate the first and last keys desplazaments.
    first_key = 0
    last_key = size - keySize + 1
        
    offset = size-(first_key+1)
    for k in range (first_key, last_key):       
        # Add key to python list
        KeysDespl.append((query[k:k+keySize],int(offset)))
        offset -= 1
        #print("Key {} -> {}".format(k, query[k:k+keySize]))
         
    #print("\rProcessing {} keys from offset {} in {} secs".format(len(Keys),offset, round(time() - gt0.value,3), end =" "))

    return KeysDespl


def cython_DoAligment(str querySequence, str referenceSequence):
    cdef float align_score
    cdef int i_start
    cdef str align_seq1,align_seq2
    
    align_seq1,align_seq2,align_score,i_start = cython_Aligment(querySequence, referenceSequence)
    return align_seq1,align_seq2,align_score, i_start

def cython_Aligment(str query_seq, str candidate_sequence):
    cdef float score, align_score
    cdef int i_start, i_end
    cdef str align_seq1,align_seq2
    cdef list i_start_indexs, i_end_indexs
    
    i_start_indexs = []
    for i_start in range(15):
        _,_,score = cython_SMalignment(candidate_sequence[i_start:],query_seq)
        i_start_indexs.append(score)
    #i_start = np.array(i_start_indexs).argmax()
    i_start = i_start_indexs.index(max(i_start_indexs))

    i_end_indexs = []
    for i_end in range(1,16):
        _,_,score = cython_SMalignment(candidate_sequence[:-i_end],query_seq)
        i_end_indexs.append(score)
    #i_end = np.array(i_end_indexs).argmax()+1
    i_end = i_end_indexs.index(max(i_end_indexs))+1
    
    candidate_sequence = candidate_sequence[i_start:-i_end]

    align_seq1,align_seq2,align_score = cython_SMalignment(candidate_sequence,query_seq)
    
    return align_seq1, align_seq2, align_score, i_start


# compare single base
def cython_SingleBaseCompare(str seq1,str seq2,int i, int j):
    if seq1[i] == seq2[j]:
        return 2
    else:
        return -1

#
# Smith–Waterman Alignment 
#
def cython_SMalignment(seq1, seq2):
    m = len(seq1)
    n = len(seq2)
    g = -3
    matrix = []
    for i in range(0, m):
        tmp = []
        for j in range(0, n):
            tmp.append(0)
        matrix.append(tmp)
    for sii in range(0, m):
        matrix[sii][0] = sii*g
    for sjj in range(0, n):
        matrix[0][sjj] = sjj*g
    for siii in range(1, m):
        for sjjj in range(1, n):
            matrix[siii][sjjj] = max(matrix[siii-1][sjjj] + g, matrix[siii - 1][sjjj - 1] + cython_SingleBaseCompare(seq1,seq2,siii, sjjj), matrix[siii][sjjj-1] + g)
    sequ1 = [seq1[m-1]]
    sequ2 = [seq2[n-1]]
    while m > 1 and n > 1:
        if max(matrix[m-1][n-2], matrix[m-2][n-2], matrix[m-2][n-1]) == matrix[m-2][n-2]:
            m -= 1
            n -= 1
            sequ1.append(seq1[m-1])
            sequ2.append(seq2[n-1])
        elif max(matrix[m-1][n-2], matrix[m-2][n-2], matrix[m-2][n-1]) == matrix[m-1][n-2]:
            n -= 1
            sequ1.append('-')
            sequ2.append(seq2[n-1])
        else:
            m -= 1
            sequ1.append(seq1[m-1])
            sequ2.append('-')
    sequ1.reverse()
    sequ2.reverse()
    align_seq1 = ''.join(sequ1)
    align_seq2 = ''.join(sequ2)
    align_score = 0.
    for k in range(0, len(align_seq1)):
        if align_seq1[k] == align_seq2[k]:
            align_score += 1
    align_score = float(align_score)/len(align_seq1)
    return align_seq1, align_seq2, align_score


def Dummy():
    return
    