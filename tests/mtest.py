import numpy as np
from sets import fusSet

dic = {"Rigotte":{"01":{"a":{"din":np.zeros((100,10,20))},
                        "b":{"din": np.zeros((100, 10, 20))}},
                  "02":{"a":{"din": np.zeros((100, 10, 20))},"b":{"din": np.zeros((100, 10, 20))}}},
       "Carotte": {"01":{"a":{"din":np.zeros((100,10,20))},
                        "b":{"din": np.zeros((100, 10, 20))}},
                  "02":{"a":{"din": np.zeros((100, 10, 20))},"b":{"din": np.zeros((100, 10, 20))}}}
       }

d = fusSet.from_dic(dic)

d2 = d[None,None,"a"]

d_e = d2.same_empty()

for x in d2:
    x["din"] = x["din"] + 10




e = d.get(["Rigotte","01","a","din"])

