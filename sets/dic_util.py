def dic_get(dic,ks):
    if len(ks) <= 1:
        return dic[ks[0]]
    return dic_get(dic[ks[0]],ks[1:])

def dic_iterate(dic,ks):
    if len(ks)<=1:
        if ks[0] == None:
            return dic
        elif type(ks[0])==list:
            return {k:dic[k] for k in ks[0]}
        return {ks[0]: dic[ks[0]]}
    if ks[0]==None:
        return {k:dic_iterate(dic[k],ks[1:]) for k in dic.keys()}
    elif type(ks[0]) == list:
        return {k: dic_iterate(dic[k],ks[1:]) for k in ks[0]}
    return dic_iterate({ks[0]: dic[ks[0]]},ks[1:])

def dic_set(dic,ks,value):
    if len(ks)<=1:
        if ks[0] == None:
            raise Exception("last key need to be not_empty")
        elif type(ks[0])==list:
            raise Exception("last key need to be single")
        dic[ks[0]] = value
        return None
    if ks[0]==None:
        for k in dic.keys():
            return dic_set(dic[k], ks[1:], value)
    elif type(ks[0]) == list:
        for k in ks[0]:
            return dic_set(dic[k],ks[1:],value)
    return dic_set(dic[ks[0]], ks[1:], value)