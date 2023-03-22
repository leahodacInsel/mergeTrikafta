import datetime
import os
from pathlib import Path
from glob import glob
from datetime import datetime
import numpy as np
import pandas as pd


'''
Input Carmen Files: 
- Bernese_CF_database2      --> Manually modified: SCILD ID completed and names/surname format (also blanks)
- Demographics2             --> Manually modified: names/surname format (also blanks)
- Start- und Enddaten CF Medikation
- UID_Name_BiDate_Sex_Study

Output:
- merged table: complete version that is then modified by deleting columns and changing names in excel

WARNING: 
- Remo Gäggeler --> in Bernese 2019, in SCILD 2017
- Lara Zosso --> difference in birthdate
'''
def write_excel(df,path, fileName):
    output_name = Path(fileName + '.xlsx')
    i = glob(path + "\\" + output_name.stem + "_[0-9]*" + output_name.suffix)
    new_output_name = f"{output_name.stem}_{len(i)+1}{output_name.suffix}"
    df.to_excel((path + '\\' + new_output_name), index=False)


def merge_Demo2_Medik(Demo2, Medik):

    dtypes = [('FirstName', str), ('LastName', str), ('DOB', datetime), ('Sex', str), ('Mutation1', str), ('Mutation2', str),
              ('Trikafta', int), ('Trikafta_start_day', int), ('Trikafta_start_month', int), ('Trikafta_start_year', int),
              ('Orkambi', int), ('Orkambi_start_day', int), ('Orkambi_start_month', int),('Orkambi_start_year', int), ('Orkambi_end_day', int),
              ('Orkambi_end_month', int), ('Orkambi_end_year', int),
              ('Symdeko', int), ('Symdeko_start_day', int), ('Symdeko_start_month', int), ('Symdeko_start_year', int),
              ('Symdeko_end_day', int), ('Symdeko_end_month', int), ('Symdeko_end_year', int)]

    df1 = pd.DataFrame(np.empty(0,dtype = dtypes))

    for index, row in Demo2.iterrows():
        df1.at[index, 'FirstName'] = row['patFirstName']
        df1.at[index, 'LastName'] = row['patSurname']
        df1.at[index, 'DOB'] = row['Pat_DOB']
        df1.at[index, 'Sex'] = row['Sex']
        df1.at[index, 'Mutation1'] = row['Mutation_1']
        df1.at[index, 'Mutation2'] = row['Mutation_2']
        df1.at[index, 'Trikafta'] = 1 # all got trikafta
        df1.at[index, 'Trikafta_start_day'] = row['Start_trikafta'].day
        df1.at[index, 'Trikafta_start_month'] = row['Start_trikafta'].month
        df1.at[index, 'Trikafta_start_year'] = row['Start_trikafta'].year
        if row['Type_Modulator'] == 'Symdeko':
            df1.at[index, 'Symdeko'] = 1
            df1.at[index, 'Symdeko_start_day'] = row['Start_modulator'].day
            df1.at[index, 'Symdeko_start_month'] = row['Start_modulator'].month
            df1.at[index, 'Symdeko_start_year'] = row['Start_modulator'].year
        elif row['Type_Modulator'] == 'Orkambi':
            df1.at[index, 'Orkambi'] = 1
            df1.at[index, 'Orkambi_start_day'] = row['Start_modulator'].day
            df1.at[index, 'Orkambi_start_month'] = row['Start_modulator'].month
            df1.at[index, 'Orkambi_start_year'] = row['Start_modulator'].year
        elif row['Type_Modulator'] != 'ohne':
            print('WARNING: other type of modulator')

    df2 = pd.DataFrame(np.empty(0,dtype = dtypes))
    for index, row in Medik.iterrows():
        df2.at[index, 'FirstName'] = row['FirstName']
        df2.at[index, 'LastName'] = row['LastName']
        df2.at[index, 'DOB'] = row['Birthday']

        df2.at[index, 'Trikafta'] = 1  # all got trikafta
        df2.at[index, 'Trikafta_start_day'] = row['Start_ELX/TEZ/IVA (day)']
        df2.at[index, 'Trikafta_start_month'] = row['Start_ELX/TEZ/IVA (month)']
        df2.at[index, 'Trikafta_start_year'] = row['Start_ELX/TEZ/IVA (year)']


        df2.at[index, 'Symdeko'] = row['TEZ/IVA (Symkevi)']
        df2.at[index, 'Symdeko_start_day'] = row['Start_TEZ/IVA (day)']
        df2.at[index, 'Symdeko_start_month'] = row['Start_TEZ/IVA (month)']
        df2.at[index, 'Symdeko_start_year'] = row['Start_TEZ/IVA (year)']
        df2.at[index, 'Symdeko_end_day'] = row['End_TEZ/IVA (day)']
        df2.at[index, 'Symdeko_end_month'] = row['End_TEZ/IVA (month)']
        df2.at[index, 'Symdeko_end_year'] = row['End_TEZ/IVA (year)']

        df2.at[index, 'Orkambi'] = row['LUM/IVA (Orkambi)']
        df2.at[index, 'Orkambi_start_day'] = row['Start_LUM/IVA (day)']
        df2.at[index, 'Orkambi_start_month'] = row['Start_LUM/IVA (month)']
        df2.at[index, 'Orkambi_start_year'] = row['Start_LUM/IVA (year)']
        df2.at[index, 'Orkambi_end_day'] = row['End_LUM/IVA (day)']
        df2.at[index, 'Orkambi_end_month'] = row['End_LUM/IVA (month)']
        df2.at[index, 'Orkambi_end_year'] = row['End_LUM/IVA (year)']


    return df1.append(df2)


if __name__ == '__main__':

    path_read = r'L:\KKM_LuFu\OfficeData\Biomedical Engineers\Lea\02.Documentation\Data Management\Carmen Data Trikafta'
    path_save = r'L:\KKM_LuFu\OfficeData\Biomedical Engineers\Lea\02.Documentation\Data Management\Carmen Data Trikafta\results'
    save_Demo2_Medik_merged = 1
    save_UID_Bernese_merged = 1
    save_Medik_UIDs_merged = 1

    Bernese_pats = pd.read_excel(os.path.join(path_read, r'Bernese_CF_database2.xlsx'))
    SCILD_pats = pd.read_excel(os.path.join(path_read, r'UID_Name_BiDate_Sex_Study.xlsx'))

    # add column which study
    Bernese_pats = Bernese_pats.assign(from_Bernese_DB= np.ones(len(Bernese_pats)))
    SCILD_pats = SCILD_pats.assign(from_SCILD_DB= np.ones(len(SCILD_pats)))

    # change sex to female and male
    for idx, row in Bernese_pats.iterrows():
        if row['patGender'] == 1:
            Bernese_pats.at[idx, 'patGender'] = 'female'
        if row['patGender'] == 0:
            Bernese_pats.at[idx, 'patGender'] = 'male'

    for idx, row in SCILD_pats.iterrows():
        if row['patPersSex'] == 1:
            SCILD_pats.at[idx, 'patPersSex'] = 'male'
        if row['patPersSex'] == 0:
            SCILD_pats.at[idx, 'patPersSex'] = 'female'

    Demo2_Medik_merged = merge_Demo2_Medik(pd.read_excel(os.path.join(path_read, r'Demographics2.xlsx')), pd.read_excel(os.path.join(path_read, r'Start- und Enddaten CF Medikation.xlsx')))

    UID_Bernese_merged = SCILD_pats.merge(Bernese_pats, how='outer', left_on=['UID'], right_on=['SCILD_correctedLHD'])


    # fill up the DOB, Last Name and First Name column
    for idx in range(len(UID_Bernese_merged)):
        if not pd.isnull(UID_Bernese_merged.at[idx, 'patBiDate']) and not pd.isnull(UID_Bernese_merged.at[idx, 'Pat_DOB']):
            if UID_Bernese_merged.at[idx, 'patBiDate'] != UID_Bernese_merged.at[idx, 'Pat_DOB']:
                print('WARNING INCORRECT MERGING - row: ', UID_Bernese_merged.at[idx, 'patNaFirstName'], UID_Bernese_merged.at[idx, 'patNaSirName'] )  # check if the merging was correct

        else:
            if pd.isnull(UID_Bernese_merged.at[idx, 'patBiDate']):
                UID_Bernese_merged.at[idx, 'patBiDate'] = UID_Bernese_merged.at[idx, 'Pat_DOB']
            if pd.isnull(UID_Bernese_merged.at[idx, 'patNaFirstName']):
                UID_Bernese_merged.at[idx, 'patNaFirstName'] = UID_Bernese_merged.at[idx, 'patFirstName']
            if pd.isnull(UID_Bernese_merged.at[idx, 'patNaSirName']):
                UID_Bernese_merged.at[idx, 'patNaSirName'] = UID_Bernese_merged.at[idx, 'patSurname']

        UID_Bernese_merged.at[idx, 'patNaFirstName'] = UID_Bernese_merged.at[idx, 'patNaFirstName']
        UID_Bernese_merged.at[idx, 'patNaSirName'] = UID_Bernese_merged.at[idx, 'patNaSirName']


    final_Medik_UID_merged = UID_Bernese_merged.merge(Demo2_Medik_merged, how='outer', left_on=['patNaFirstName', 'patNaSirName'], right_on=['FirstName','LastName'])

    # merge gender columns
    for idx, row in final_Medik_UID_merged.iterrows():
        if not pd.isnull(row['patGender']):
            if pd.isnull(row['patPersSex']):
                final_Medik_UID_merged.at[idx, 'patPersSex'] = row['patGender']
            else:
                if not (row['patGender'] == row['patPersSex']):
                    print('WARNING: GENDER DONT CORRESPOND FOR ', row['patNaFirstName'], row['patNaSirName'])



    if save_Demo2_Medik_merged:
        write_excel(Demo2_Medik_merged, path_save, 'Demographics2_StartEnddatenMedikation_merged')

    if save_UID_Bernese_merged:
        write_excel(UID_Bernese_merged, path_save, 'UID_Bernese_merged')

    if save_Medik_UIDs_merged
        write_excel(final_Medik_UID_merged, path_save, 'final_Medik_UID_merged')



    print("... jusqu'ici tout va bien")