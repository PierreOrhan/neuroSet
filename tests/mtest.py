import numpy as np
from sets.nSet import nSet

dic = {"Rigotte":{"01":{"a":{"din":np.zeros((100,10,20))},
                        "b":{"din": np.zeros((100, 10, 20))}},
                  "02":{"a":{"din": np.zeros((100, 10, 20))},"b":{"din": np.zeros((100, 10, 20))}}},
       "Carotte": {"01":{"a":{"din":np.zeros((100,10,20))},
                        "b":{"din": np.zeros((100, 10, 20))}},
                  "02":{"a":{"din": np.zeros((100, 10, 20))},"b":{"din": np.zeros((100, 10, 20))}},
                   "03":{"a":{"din": np.zeros((100, 10, 20))},"b":{"din": np.zeros((100, 10, 20))}}}
       }

d = nSet.from_dic(dic,3)

b = d.merge_level(2,["din"],"merge",np.concatenate,axis=0)

d2 = d[None,None,"a"]

d_e = d2.same_empty()

for x in d2:
    x["din"] = x["din"] + 10

e = d.get(["Rigotte","01","a","din"])

### test the element seting:
from sets.nSet import product_dicempty
dp = product_dicempty(d,d)


## TODO: unit tests...