def hhhh(x,y):
    if(x<y):
        smaller=x
    else:
        smaller=y

    for i in range(1,smaller):
        if ((x%i==0) and (y%i==0)):
            hcf=i
            
    return hcf

print hhhh(80,100)



# n1=40
# list_l=[]
# for i in range(1,n1):
#     if(n1%i==0):
#         list_l.extend([i])
        


# print list_l