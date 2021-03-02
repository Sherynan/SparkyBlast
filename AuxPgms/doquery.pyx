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
    KeysDespl = []
    
    #tg1 = time()
    # Calculate the first and last keys desplazaments.
    first_key = 0
    last_key = size - keySize + 1
        
    offset = size-(first_key+1)
    for k in range (first_key, last_key):       
        # Add key to python list
        KeysDespl.append((query[k:k+keySize],int(offset)))
        offset -= 1
        
    #print("\rProcessing {} keys from offset {} in {} secs".format(len(Keys),offset, round(time() - gt0.value,3), end =" "))
    
    return KeysDespl
