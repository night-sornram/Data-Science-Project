import pandas as pd

df = pd.read_csv('./scopus_messages.csv')

select = int(input("Enter the path of the file you want to clean (1:ML/AI) (2:Vizs): "))

df.drop_duplicates(inplace=True)

match select:
    case 1:
        file_path = "ML_AI.csv"
        df.drop(columns=['code','prism_type','subject_area','ref_count','publisher','affiliation','authors'], inplace=True)
        df.dropna(subset=['title','abstract','publication_date','keywords'], inplace=True)
        df.to_csv(file_path, index=False)
    case 2:
        file_path = "Vizs.csv"
        df.dropna(inplace=True)
        df.to_csv(file_path, index=False)
    case _:
        print("Invalid selection")
        exit()
        
import pandas as pd

df = pd.read_csv('./scopus_messages.csv')

