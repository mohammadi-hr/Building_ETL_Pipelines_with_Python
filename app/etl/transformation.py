import pandas as pd
import numpy as np

df = pd.DataFrame({
    "pages_1" : [100, np.nan, 157, 258],
    "pages_2" : [12, 30, 15, np.nan],
    "pages_3" : [np.nan, 45, 47, 98],
    "pages_4" : [752, 752, 774, 758],
})

df.fillna(df.mean(), inplace=True)

print(df)